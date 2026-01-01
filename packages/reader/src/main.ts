import Fastify from 'fastify';
import pg from 'pg';
import Redis from 'ioredis';
import { connect, NatsConnection, StringCodec, JetStreamClient, AckPolicy, DeliverPolicy } from 'nats';

const fastify = Fastify({ logger: true });
const sc = StringCodec();

let db: pg.Pool;
let redis: Redis;
let nc: NatsConnection;

const CACHE_TTL_SECONDS = 30;

interface User {
  id: string;
  name: string;
  email: string;
  created_at: string;
}

interface Order {
  id: string;
  user_id: string;
  items: Array<{ productId: string; quantity: number; price: number }>;
  total: string;
  created_at: string;
}

async function getCached<T>(key: string, fetcher: () => Promise<T>): Promise<T> {
  const cached = await redis.get(key);
  if (cached) {
    fastify.log.info({ key }, 'Cache hit');
    return JSON.parse(cached);
  }

  fastify.log.info({ key }, 'Cache miss');
  const data = await fetcher();
  await redis.setex(key, CACHE_TTL_SECONDS, JSON.stringify(data));
  return data;
}

fastify.get('/health', async () => {
  return {
    status: 'ok',
    redis: redis.status === 'ready',
  };
});

fastify.get('/users', async () => {
  const users = await getCached<User[]>('users:all', async () => {
    const result = await db.query<User>(
      'SELECT id, name, email, created_at FROM users ORDER BY created_at DESC'
    );
    return result.rows;
  });

  return { users };
});

fastify.get<{ Querystring: { userId?: string } }>('/orders', async (request) => {
  const { userId } = request.query;
  const cacheKey = userId ? `orders:user:${userId}` : 'orders:all';

  const orders = await getCached<Order[]>(cacheKey, async () => {
    if (userId) {
      const result = await db.query<Order>(
        'SELECT id, user_id, items, total, created_at FROM orders WHERE user_id = $1 ORDER BY created_at DESC',
        [userId]
      );
      return result.rows;
    }

    const result = await db.query<Order>(
      'SELECT id, user_id, items, total, created_at FROM orders ORDER BY created_at DESC'
    );
    return result.rows;
  });

  return { orders };
});

// Debezium CDC event structure (after ExtractNewRecordState transform)
interface CdcEvent {
  // Record fields
  id?: string;
  user_id?: string;
  // Metadata added by transform
  __op: 'c' | 'u' | 'd' | 'r'; // create, update, delete, read (snapshot)
  __table: string;
  __source_ts_ms: number;
  // For deletes, __deleted field is added
  __deleted?: string;
}

async function startCdcConsumer(js: JetStreamClient) {
  // Wait for Debezium to create the stream (it auto-creates on startup)
  // We'll retry until the stream exists
  const streamName = 'DebeziumStream';
  const maxRetries = 30;
  const retryDelay = 2000;

  for (let i = 0; i < maxRetries; i++) {
    try {
      const jsm = await nc.jetstreamManager();
      await jsm.streams.info(streamName);
      fastify.log.info(`Found CDC stream: ${streamName}`);
      break;
    } catch (err) {
      if (i === maxRetries - 1) {
        fastify.log.error('CDC stream not found after max retries - cache invalidation disabled');
        return;
      }
      fastify.log.info(`Waiting for CDC stream (attempt ${i + 1}/${maxRetries})...`);
      await new Promise((resolve) => setTimeout(resolve, retryDelay));
    }
  }

  // Create a durable consumer for cache invalidation
  const consumerName = 'cache-invalidator';

  try {
    const jsm = await nc.jetstreamManager();
    await jsm.consumers.add(streamName, {
      durable_name: consumerName,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.New, // Only new messages, not historical
      filter_subjects: ['cdc.public.users', 'cdc.public.orders'],
    });
    fastify.log.info(`Created CDC consumer: ${consumerName}`);
  } catch (err: unknown) {
    // Consumer might already exist
    if (err instanceof Error && !err.message.includes('already exists')) {
      fastify.log.warn({ err }, 'Error creating CDC consumer (may already exist)');
    }
  }

  const consumer = await js.consumers.get(streamName, consumerName);
  const messages = await consumer.consume();

  fastify.log.info('Listening for CDC events from Debezium');

  for await (const msg of messages) {
    try {
      const subject = msg.subject; // e.g., "cdc.public.users"
      const table = subject.split('.').pop(); // "users" or "orders"
      const event: CdcEvent = JSON.parse(sc.decode(msg.data));

      fastify.log.info({ table, op: event.__op }, 'CDC event received');

      if (table === 'users') {
        await redis.del('users:all');
        fastify.log.info('Invalidated users:all');
      } else if (table === 'orders') {
        await redis.del('orders:all');
        // Invalidate user-specific order cache
        const userId = event.user_id;
        if (userId) {
          await redis.del(`orders:user:${userId}`);
          fastify.log.info({ userId }, 'Invalidated orders cache');
        }
      }

      msg.ack();
    } catch (err) {
      fastify.log.error({ err }, 'Failed to process CDC event');
      msg.nak();
    }
  }
}

async function main() {
  const natsUrl = process.env.NATS_URL || 'nats://localhost:4222';
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  const databaseUrl = process.env.DATABASE_URL || 'postgres://jetstream:jetstream@localhost:5432/jetstream';
  const port = parseInt(process.env.PORT || '3001', 10);
  const host = process.env.HOST || '0.0.0.0';

  nc = await connect({ servers: natsUrl });
  fastify.log.info('Connected to NATS');

  const js = nc.jetstream();

  redis = new Redis(redisUrl);
  db = new pg.Pool({ connectionString: databaseUrl });

  await redis.ping();
  fastify.log.info('Connected to Redis');

  await db.query('SELECT 1');
  fastify.log.info('Connected to Postgres');

  // Start CDC consumer for cache invalidation (Debezium → JetStream)
  startCdcConsumer(js);

  await fastify.listen({ port, host });

  const shutdown = async () => {
    console.log('Shutting down...');
    await fastify.close();
    await nc.close();
    await redis.quit();
    await db.end();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch(console.error);
