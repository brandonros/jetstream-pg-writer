import { connect, NatsConnection, JetStreamClient, StringCodec, JsMsg } from 'nats';
import { Pool } from 'pg';
import { WriteRequest, WriteResponse } from '../shared/types';

const sc = StringCodec();

class Consumer {
  private nc!: NatsConnection;
  private js!: JetStreamClient;
  private db: Pool;

  constructor() {
    this.db = new Pool({
      connectionString: process.env.DATABASE_URL || 'postgres://jetstream:jetstream@localhost:5432/jetstream',
    });
  }

  async connect() {
    const natsUrl = process.env.NATS_URL || 'nats://localhost:4222';
    
    this.nc = await connect({ servers: natsUrl });
    this.js = this.nc.jetstream();

    // Create durable consumer
    const jsm = await this.nc.jetstreamManager();
    try {
      await jsm.consumers.add('WRITES', {
        durable_name: 'db-writer',
        ack_policy: 'explicit',
        max_deliver: 3,
        ack_wait: 30_000_000_000,
      });
    } catch (e) {
      // Consumer already exists
    }

    console.log(`Consumer connected to ${natsUrl}`);
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
      await this.idempotentWrite(request);
      
      response = {
        success: true,
        operationId: request.operationId,
      };

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

  private async idempotentWrite(request: WriteRequest): Promise<void> {
    await this.db.query(
      `INSERT INTO writes (operation_id, table_name, data, created_at)
       VALUES ($1, $2, $3, NOW())
       ON CONFLICT (operation_id) DO NOTHING`,
      [request.operationId, request.table, JSON.stringify(request.data)]
    );
  }

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

  async close() {
    await this.db.end();
    await this.nc.close();
  }
}

async function main() {
  const consumer = new Consumer();
  await consumer.connect();
  
  process.on('SIGINT', async () => {
    console.log('Shutting down...');
    await consumer.close();
    process.exit(0);
  });

  await consumer.start();
}

main().catch(console.error);