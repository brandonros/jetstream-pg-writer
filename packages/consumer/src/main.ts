import { connect, NatsConnection, JetStreamClient, StringCodec, JsMsg, AckPolicy } from 'nats';
import pg from 'pg';
import type { WriteRequest, WriteResponse, UserData, OrderData } from '@jetstream-pg-writer/shared';

const { Pool } = pg;
const sc = StringCodec();

class Consumer {
  private nc!: NatsConnection;
  private js!: JetStreamClient;
  private db: pg.Pool;

  constructor() {
    this.db = new Pool({
      connectionString: process.env.DATABASE_URL || 'postgres://jetstream:jetstream@localhost:5432/jetstream',
    });
  }

  async connect() {
    const natsUrl = process.env.NATS_URL || 'nats://localhost:4222';

    this.nc = await connect({ servers: natsUrl });
    this.js = this.nc.jetstream();

    const jsm = await this.nc.jetstreamManager();

    // Ensure stream exists
    try {
      await jsm.streams.add({
        name: 'WRITES',
        subjects: ['writes.>'],
      });
    } catch {
      // Stream already exists
    }

    // Create durable consumer
    try {
      await jsm.consumers.add('WRITES', {
        durable_name: 'db-writer',
        ack_policy: AckPolicy.Explicit,
        max_deliver: 3,
        ack_wait: 30_000_000_000,
      });
    } catch {
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

    console.log(`Processing operation: ${request.operationId} for table: ${request.table}`);

    let response: WriteResponse;

    try {
      const written = await this.writeToTable(request);

      response = {
        success: true,
        operationId: request.operationId,
      };

      if (!written) {
        console.log(`Duplicate operation skipped: ${request.operationId}`);
      } else {
        console.log(`Completed: ${request.operationId}`);
      }

      msg.ack();
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
        msg.ack(); // Don't retry non-transient errors
      }
    }

    if (replyTo) {
      this.nc.publish(replyTo, sc.encode(JSON.stringify(response)));
    }
  }

  private async writeToTable(request: WriteRequest): Promise<boolean> {
    const client = await this.db.connect();

    try {
      await client.query('BEGIN');

      // Check idempotency
      const check = await client.query(
        'SELECT 1 FROM processed_operations WHERE operation_id = $1',
        [request.operationId]
      );

      if (check.rows.length > 0) {
        await client.query('ROLLBACK');
        return false; // Already processed
      }

      // Write to the actual table
      switch (request.table) {
        case 'users':
          await this.insertUser(client, request.data as unknown as UserData);
          break;
        case 'orders':
          await this.insertOrder(client, request.operationId, request.data as unknown as OrderData);
          break;
        default:
          throw new Error(`Unsupported table: ${request.table}`);
      }

      // Mark as processed
      await client.query(
        'INSERT INTO processed_operations (operation_id, created_at) VALUES ($1, NOW())',
        [request.operationId]
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

  private async insertUser(client: pg.PoolClient, data: UserData) {
    await client.query(
      'INSERT INTO users (name, email) VALUES ($1, $2)',
      [data.name, data.email]
    );
  }

  private async insertOrder(client: pg.PoolClient, orderId: string, data: OrderData) {
    await client.query(
      'INSERT INTO orders (id, user_id, items, total) VALUES ($1, $2, $3, $4)',
      [orderId, data.userId, JSON.stringify(data.items), data.total]
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

  process.on('SIGTERM', async () => {
    console.log('Shutting down...');
    await consumer.close();
    process.exit(0);
  });

  await consumer.start();
}

main().catch(console.error);
