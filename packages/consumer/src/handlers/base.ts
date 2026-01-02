import { JetStreamClient, JsMsg, StringCodec, NatsConnection } from 'nats';
import pg from 'pg';
import type { Redis } from 'ioredis';
import type { WriteRequest, WriteResponse } from '@jetstream-pg-writer/shared';

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

  abstract readonly table: string;
  abstract readonly consumerName: string;

  constructor(nc: NatsConnection, js: JetStreamClient, db: pg.Pool, redis: Redis) {
    this.nc = nc;
    this.js = js;
    this.db = db;
    this.redis = redis;
  }

  async start() {
    const consumer = await this.js.consumers.get('WRITES', this.consumerName);
    const messages = await consumer.consume();

    console.log(`${this.consumerName} handler started, listening for writes.${this.table}`);

    for await (const msg of messages) {
      await this.handleMessage(msg);
    }
  }

  private async handleMessage(msg: JsMsg) {
    const request: WriteRequest = JSON.parse(sc.decode(msg.data));
    const replyTo = msg.headers?.get('Reply-To');

    console.log(`[${this.table}] Processing: ${request.operationId}`);

    let response: WriteResponse;

    try {
      const result = await this.processWrite(request.operationId, request.data as T);

      response = {
        success: true,
        operationId: request.operationId,
        entityId: result.entityId,
      };

      if (result.entityId) {
        console.log(`[${this.table}] Completed: ${request.operationId} -> ${result.entityId}`);
      } else {
        console.log(`[${this.table}] Duplicate skipped: ${request.operationId}`);
      }

      msg.ack();
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : 'Unknown error';
      console.error(`[${this.table}] Failed: ${request.operationId}`, errMsg);

      response = {
        success: false,
        operationId: request.operationId,
        error: errMsg,
      };

      if (this.isRetryable(error)) {
        msg.nak();
      } else {
        msg.ack();
      }
    }

    if (replyTo) {
      this.nc.publish(replyTo, sc.encode(JSON.stringify(response)));
    }
  }

  private async processWrite(operationId: string, data: T): Promise<{ entityId: string }> {
    const client = await this.db.connect();

    try {
      await client.query('BEGIN');

      // Generate entity ID
      const entityId = crypto.randomUUID();

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
          const existing = await client.query(
            'SELECT entity_id FROM write_operations WHERE operation_id = $1',
            [operationId]
          );
          return { entityId: existing.rows[0]?.entity_id ?? '' };
        }
        throw error;
      }

      // 2. Perform the actual domain insert
      await this.insert(client, entityId, data);

      await client.query('COMMIT');

      // 3. Invalidate cache synchronously (before returning to client)
      await this.invalidateCache(entityId, data);
      console.log(`[${this.table}] Cache invalidated`);

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
