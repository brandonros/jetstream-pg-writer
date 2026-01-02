import { randomUUID } from 'node:crypto';
import { JetStreamClient, JsMsg, StringCodec, NatsConnection } from 'nats';
import pg from 'pg';
import type { Redis } from 'ioredis';
import type { WriteRequest } from '@jetstream-pg-writer/shared';
import type { Logger } from '@jetstream-pg-writer/shared/logger';
import { invalidateNamespace } from '@jetstream-pg-writer/shared/cache';

const sc = StringCodec();

// Consumer max_deliver is 3, so redeliveryCount 2 = final attempt (0, 1, 2)
const MAX_REDELIVERY_COUNT = 2;

// Postgres error codes - safelist of retryable errors
// Using safelist (only retry known-transient) is safer than blocklist
const PG_UNIQUE_VIOLATION = '23505';
const PG_RETRYABLE_CODES = new Set([
  '08000', // connection_exception
  '08003', // connection_does_not_exist
  '08006', // connection_failure
  '40001', // serialization_failure (deadlock)
  '40P01', // deadlock_detected
  '53300', // too_many_connections
  '57P01', // admin_shutdown
  '57P02', // crash_shutdown
  '57P03', // cannot_connect_now
]);

export abstract class BaseHandler<T> {
  protected js: JetStreamClient;
  protected nc: NatsConnection;
  protected db: pg.Pool;
  protected redis: Redis;
  protected log: Logger;

  abstract readonly table: string;
  abstract readonly consumerName: string;

  constructor(nc: NatsConnection, js: JetStreamClient, db: pg.Pool, redis: Redis, log: Logger) {
    this.nc = nc;
    this.js = js;
    this.db = db;
    this.redis = redis;
    this.log = log;
  }

  async start() {
    const consumer = await this.js.consumers.get('WRITES', this.consumerName);
    const messages = await consumer.consume();

    this.log.info({ handler: this.consumerName, table: this.table }, 'Handler started');

    for await (const msg of messages) {
      await this.handleMessage(msg);
    }
  }

  private async handleMessage(msg: JsMsg) {
    const request: WriteRequest = JSON.parse(sc.decode(msg.data));

    this.log.info({ table: this.table, operationId: request.operationId }, 'Processing write');

    try {
      await this.processWrite(request.operationId, request.data as T);
      msg.ack();
    } catch (error) {
      const redeliveryCount = msg.info.redeliveryCount;
      const isLastAttempt = redeliveryCount >= MAX_REDELIVERY_COUNT;

      // Retryable errors (connection, timeout) → nak for retry (unless exhausted)
      // Non-retryable errors (constraint violations) → ack (failure already recorded in DB)
      if (this.isRetryable(error)) {
        if (isLastAttempt) {
          // Final attempt failed - send to DLQ for investigation/replay.
          // We don't recordFailure() here because if DB is unreachable (the likely cause),
          // that call would also fail. Client sees 'pending' but can't poll anyway.
          // When DB recovers, ops can replay from DLQ.
          await this.publishToDlq(msg, request, error);
          msg.ack();
        } else {
          this.log.warn({ table: this.table, operationId: request.operationId, redeliveryCount, err: error }, 'Retryable error, will retry');
          msg.nak(1000);
        }
      } else {
        this.log.error({ table: this.table, operationId: request.operationId, err: error }, 'Non-retryable error, failure recorded');
        msg.ack();
      }
    }
  }

  private async processWrite(operationId: string, data: T): Promise<void> {
    const entityId = randomUUID();
    const client = await this.db.connect();

    try {
      await client.query('BEGIN');

      // 1. Idempotency check - insert with status='pending'
      try {
        await client.query(
          `INSERT INTO write_operations (operation_id, entity_table, entity_id, op_type, status)
           VALUES ($1, $2, $3, $4, $5)`,
          [operationId, this.table, entityId, 'create', 'pending']
        );
      } catch (error) {
        await client.query('ROLLBACK');
        // Duplicate operation - already processed (or in progress)
        if (error instanceof Error && 'code' in error && error.code === PG_UNIQUE_VIOLATION) {
          this.log.info({ table: this.table, operationId }, 'Duplicate operation, skipping');
          return;
        }
        throw error;
      }

      // 2. Perform the actual domain insert
      await this.insert(client, entityId, data);

      // 3. Mark as completed
      await client.query(
        `UPDATE write_operations SET status = $1, completed_at = NOW() WHERE operation_id = $2`,
        ['completed', operationId]
      );

      await client.query('COMMIT');
      this.log.info({ table: this.table, operationId, entityId }, 'Write completed');

      // 4. Invalidate cache (non-fatal)
      try {
        await this.invalidateCache(operationId, entityId, data);
      } catch (error) {
        this.log.warn({ table: this.table, operationId, err: error }, 'Cache invalidation failed (non-fatal)');
      }
    } catch (error) {
      await client.query('ROLLBACK');

      // Record failure for non-retryable errors so client can poll for result
      // Wrap in try/catch to preserve original error context
      if (!this.isRetryable(error)) {
        const errMsg = error instanceof Error ? error.message : 'Unknown error';
        try {
          await this.recordFailure(operationId, entityId, errMsg);
        } catch (recordErr) {
          // Log but don't lose the original error
          this.log.error({ table: this.table, operationId, recordErr }, 'Failed to record failure (original error preserved)');
        }
      }

      throw error;
    } finally {
      client.release();
    }
  }

  private async recordFailure(operationId: string, entityId: string, error: string): Promise<void> {
    await this.db.query(
      `INSERT INTO write_operations (operation_id, entity_table, entity_id, op_type, status, error, completed_at)
       VALUES ($1, $2, $3, $4, $5, $6, NOW())
       ON CONFLICT (operation_id) DO UPDATE SET status = $5, error = $6, completed_at = NOW()`,
      [operationId, this.table, entityId, 'create', 'failed', error]
    );
    this.log.info({ table: this.table, operationId }, 'Failure recorded');
  }

  protected abstract insert(client: pg.PoolClient, entityId: string, data: T): Promise<void>;

  // Each handler defines which cache keys to invalidate
  protected abstract invalidateCache(operationId: string, entityId: string, data: T): Promise<void>;

  // Expose invalidateNamespace helper to subclasses
  protected invalidateNamespace(namespace: 'users' | 'orders'): Promise<number> {
    return invalidateNamespace(this.redis, namespace);
  }

  // Safelist approach: only retry known-transient Postgres errors
  // Safer than blocklist - unknown errors fail fast rather than retry forever
  private isRetryable(error: unknown): boolean {
    if (error instanceof Error && 'code' in error && typeof error.code === 'string') {
      return PG_RETRYABLE_CODES.has(error.code);
    }
    return false;
  }

  /**
   * Publish failed message to DLQ for investigation/manual replay.
   * Returns only after JetStream confirms the message is persisted (PubAck).
   * Caller should ack the original message after this succeeds - we've handled
   * it by routing to DLQ, so the original stream should stop tracking it.
   */
  private async publishToDlq(msg: JsMsg, request: WriteRequest, error: unknown): Promise<void> {
    const dlqPayload = {
      originalSubject: msg.subject,
      operationId: request.operationId,
      table: request.table,
      data: request.data,
      queuedAt: request.queuedAt,
      error: error instanceof Error ? error.message : 'Unknown error',
      failedAt: new Date().toISOString(),
      redeliveryCount: msg.info.redeliveryCount,
    };

    // js.publish returns PubAck - throws if stream rejects or times out
    const ack = await this.js.publish(
      `writes-dlq.${this.table}`,
      sc.encode(JSON.stringify(dlqPayload))
    );

    this.log.error(
      { table: this.table, operationId: request.operationId, dlqSeq: ack.seq, redeliveryCount: msg.info.redeliveryCount },
      'Message sent to DLQ after exhausting retries'
    );
  }
}
