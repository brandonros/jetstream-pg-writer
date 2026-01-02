import { randomUUID } from 'node:crypto';
import { JetStreamClient, JsMsg, StringCodec, NatsConnection } from 'nats';
import pg from 'pg';
import type { Redis } from 'ioredis';
import type { WriteRequest, WriteResponse } from '@jetstream-pg-writer/shared';
import type { Logger } from '@jetstream-pg-writer/shared/logger';

const sc = StringCodec();

// Postgres error code for unique_violation
const PG_UNIQUE_VIOLATION = '23505';

export interface InsertResult {
  entityId: string;
}

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
    const replyTo = msg.headers?.get('Reply-To');

    this.log.info({ table: this.table, operationId: request.operationId }, 'Processing write');

    let response: WriteResponse;
    let shouldAck = true;

    try {
      const result = await this.processWrite(request.operationId, request.data as T);

      response = {
        success: true,
        operationId: request.operationId,
        entityId: result.entityId,
      };

      if (result.entityId) {
        this.log.info({ table: this.table, operationId: request.operationId, entityId: result.entityId }, 'Write completed');
      } else {
        this.log.info({ table: this.table, operationId: request.operationId }, 'Duplicate skipped');
      }
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : 'Unknown error';
      this.log.error({ table: this.table, operationId: request.operationId, err: errMsg }, 'Write failed');

      response = {
        success: false,
        operationId: request.operationId,
        error: errMsg,
      };

      // TRADEOFF: No Dead Letter Queue. After max_deliver (3 retries), messages are dropped.
      // This is acceptable because:
      // - Writes block until ack'd - client already knows if their write failed
      // - DLQ would store failures that clients already received error responses for
      // - Non-retryable errors (constraint violations, bad data) aren't worth retrying anyway
      // - Retryable errors (connection issues) usually succeed on retry
      //
      // If async writes are added later, implement DLQ for undeliverable messages.
      shouldAck = !this.isRetryable(error);
    }

    // Reply BEFORE ack/nak. If we crash after reply but before ack:
    // - Client got confirmation (good)
    // - JetStream redelivers (no ack received)
    // - Idempotency check returns existing entity_id
    // - Duplicate reply sent (harmless - same content, client may have timed out anyway)
    //
    // If we ack'd first and crashed before reply:
    // - Write succeeded, message won't redeliver
    // - Client never learns outcome, times out, may retry with new idempotency key
    // - That's the bug we're avoiding.
    if (replyTo) {
      this.nc.publish(replyTo, sc.encode(JSON.stringify(response)));
    }

    if (shouldAck) {
      msg.ack();
    } else {
      msg.nak();
    }
  }

  private async processWrite(operationId: string, data: T): Promise<{ entityId: string }> {
    const client = await this.db.connect();

    try {
      await client.query('BEGIN');

      // Generate entity ID
      const entityId = randomUUID();

      // 1. Idempotency check - insert into write_operations first
      try {
        await client.query(
          `INSERT INTO write_operations (operation_id, entity_table, entity_id, op_type)
           VALUES ($1, $2, $3, $4)`,
          [operationId, this.table, entityId, 'create']
        );
      } catch (error) {
        await client.query('ROLLBACK');
        // Duplicate operation - look up existing entity_id
        if (error instanceof Error && 'code' in error && error.code === PG_UNIQUE_VIOLATION) {
          const existing = await client.query<{ entity_id: string }>(
            'SELECT entity_id FROM write_operations WHERE operation_id = $1',
            [operationId]
          );
          const entityId = existing.rows[0]?.entity_id;
          if (!entityId) {
            throw new Error(`Idempotency record not found for operation ${operationId}`);
          }
          return { entityId };
        }
        throw error;
      }

      // 2. Perform the actual domain insert
      await this.insert(client, entityId, data);

      await client.query('COMMIT');

      // 3. Invalidate cache synchronously so the client sees their own write immediately.
      // CDC (read-api) also invalidates cache for external changes (migrations, manual SQL, other services).
      // Both are needed: sync for read-your-writes consistency, CDC for external consistency.
      await this.invalidateCache(entityId, data);
      this.log.info({ table: this.table }, 'Cache invalidated');

      return { entityId };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  protected abstract insert(client: pg.PoolClient, entityId: string, data: T): Promise<void>;

  // Each handler defines which cache keys to invalidate
  protected abstract invalidateCache(entityId: string, data: T): Promise<void>;

  private isRetryable(error: unknown): boolean {
    if (error instanceof Error) {
      const msg = error.message.toLowerCase();
      return (
        msg.includes('connection') ||
        msg.includes('timeout') ||
        msg.includes('deadlock') ||
        msg.includes('too many connections')
      );
    }
    return false;
  }
}
