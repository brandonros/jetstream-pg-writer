import { JetStreamClient, JsMsg, StringCodec, NatsConnection } from 'nats';
import pg from 'pg';
import type { WriteRequest, WriteResponse } from '@jetstream-pg-writer/shared';

const sc = StringCodec();

export abstract class BaseHandler<T> {
  protected js: JetStreamClient;
  protected nc: NatsConnection;
  protected db: pg.Pool;

  abstract readonly table: string;
  abstract readonly consumerName: string;

  constructor(nc: NatsConnection, js: JetStreamClient, db: pg.Pool) {
    this.nc = nc;
    this.js = js;
    this.db = db;
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
      const written = await this.processWrite(request.operationId, request.data as T);

      response = {
        success: true,
        operationId: request.operationId,
      };

      if (!written) {
        console.log(`[${this.table}] Duplicate skipped: ${request.operationId}`);
      } else {
        console.log(`[${this.table}] Completed: ${request.operationId}`);
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

  private async processWrite(operationId: string, data: T): Promise<boolean> {
    const client = await this.db.connect();

    try {
      await client.query('BEGIN');

      const check = await client.query(
        'SELECT 1 FROM processed_operations WHERE operation_id = $1',
        [operationId]
      );

      if (check.rows.length > 0) {
        await client.query('ROLLBACK');
        return false;
      }

      await this.insert(client, operationId, data);

      await client.query(
        'INSERT INTO processed_operations (operation_id, created_at) VALUES ($1, NOW())',
        [operationId]
      );

      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  protected abstract insert(client: pg.PoolClient, operationId: string, data: T): Promise<void>;

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
