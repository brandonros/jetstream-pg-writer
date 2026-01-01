import { connect, NatsConnection, JetStreamClient, StringCodec, JsMsg } from 'nats';
import { WriteRequest, WriteResponse } from '../shared/types';

const sc = StringCodec();

// Simulating your DB - in reality this is Postgres/whatever
interface Database {
  query(sql: string, params: unknown[]): Promise<{ rowCount: number }>;
}

class Consumer {
  private nc!: NatsConnection;
  private js!: JetStreamClient;
  private db: Database;

  constructor(db: Database) {
    this.db = db;
  }

  async connect() {
    this.nc = await connect({ servers: 'localhost:4222' });
    this.js = this.nc.jetstream();

    // Create durable consumer
    const jsm = await this.nc.jetstreamManager();
    try {
      await jsm.consumers.add('WRITES', {
        durable_name: 'db-writer',
        ack_policy: 'explicit',      // We control when to ack
        max_deliver: 3,              // Retry up to 3 times
        ack_wait: 30_000_000_000,    // 30s in nanos - time before redelivery
      });
    } catch (e) {
      // Consumer already exists
    }
  }

  async start() {
    const consumer = await this.js.consumers.get('WRITES', 'db-writer');
    const messages = await consumer.consume();

    console.log('Consumer started, waiting for messages...');

    for await (const msg of messages) {
      await this.handleMessage(msg);
    }
  }

  private async handleMessage(msg: JsMsg) {
    const request: WriteRequest = JSON.parse(sc.decode(msg.data));
    const replyTo = msg.headers?.get('Reply-To');
    
    console.log(`Processing operation: ${request.operationId}`);

    let response: WriteResponse;

    try {
      // Idempotent write - use operationId to dedupe at DB level
      await this.idempotentWrite(request);
      
      response = {
        success: true,
        operationId: request.operationId,
      };

      // Only ACK after successful DB write
      msg.ack();
      console.log(`Completed: ${request.operationId}`);

    } catch (error) {
      const errMsg = error instanceof Error ? error.message : 'Unknown error';
      console.error(`Failed: ${request.operationId}`, errMsg);

      response = {
        success: false,
        operationId: request.operationId,
        error: errMsg,
      };

      // Determine if retryable
      if (this.isRetryable(error)) {
        // NAK triggers redelivery (with backoff based on consumer config)
        msg.nak();
      } else {
        // Non-retryable - ack to prevent infinite retry, but report failure
        msg.ack();
      }
    }

    // Reply to producer if they're waiting
    if (replyTo) {
      this.nc.publish(replyTo, sc.encode(JSON.stringify(response)));
    }
  }

  private async idempotentWrite(request: WriteRequest): Promise<void> {
    // Option 1: Use operationId as primary key / unique constraint
    // INSERT ... ON CONFLICT (operation_id) DO NOTHING
    //
    // Option 2: Separate idempotency table
    // 
    // This is pseudo-code - adjust for your DB/ORM
    
    await this.db.query(
      `INSERT INTO ${request.table} (operation_id, data, created_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (operation_id) DO NOTHING`,
      [request.operationId, JSON.stringify(request.data)]
    );
    
    // Note: ON CONFLICT DO NOTHING means duplicate operations silently succeed
    // which is exactly what you want for idempotency
  }

  private isRetryable(error: unknown): boolean {
    // Retry on transient failures
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

  async close() {
    await this.nc.close();
  }
}
