import { JetStreamClient, JsMsg, StringCodec, NatsConnection, ConsumerMessages } from 'nats';
import pg from 'pg';
import type { WriteRequest, WriteResponse } from '@jetstream-pg-writer/shared';

const sc = StringCodec();

// Postgres error code for unique_violation
const PG_UNIQUE_VIOLATION = '23505';

// Timeout waiting for CDC confirmation (ms)
const CDC_CONFIRM_TIMEOUT_MS = 10_000;

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
    // Subscribe BEFORE insert to avoid race with CDC confirmation
    const { promise, cancel } = this.waitForCdcConfirmation(operationId);

    try {
      await this.insert(this.db, operationId, data);
    } catch (error) {
      cancel();
      // PK/unique violation means duplicate - treat as success (no need to wait for CDC)
      if (error instanceof Error && 'code' in error && error.code === PG_UNIQUE_VIOLATION) {
        return false;
      }
      throw error;
    }

    // Wait for CDC to confirm cache invalidation before returning
    await promise;
    return true;
  }

  private waitForCdcConfirmation(operationId: string): { promise: Promise<void>; cancel: () => void } {
    const subject = `cdc.confirm.${operationId}`;
    let cancelled = false;
    let messages: ConsumerMessages | undefined;

    const promise = (async () => {
      // Create ephemeral consumer filtered to this specific confirmation
      const consumer = await this.js.consumers.get('CDC_CONFIRMS', {
        filterSubjects: [subject],
      });

      messages = await consumer.fetch({ max_messages: 1, expires: CDC_CONFIRM_TIMEOUT_MS });

      for await (const msg of messages) {
        if (cancelled) return;
        msg.ack();
        console.log(`[${this.table}] CDC confirmed: ${operationId}`);
        return;
      }

      if (!cancelled) {
        throw new Error(`CDC confirmation timeout after ${CDC_CONFIRM_TIMEOUT_MS}ms`);
      }
    })();

    const cancel = () => {
      cancelled = true;
      messages?.stop();
    };

    return { promise, cancel };
  }

  protected abstract insert(db: pg.Pool, operationId: string, data: T): Promise<void>;

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
