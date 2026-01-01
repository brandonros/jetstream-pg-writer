import Fastify from 'fastify';
import pg from 'pg';
import Redis from 'ioredis';

const fastify = Fastify({ logger: true });

let db: pg.Pool;
let redis: Redis;

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

async function main() {
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  const databaseUrl = process.env.DATABASE_URL || 'postgres://jetstream:jetstream@localhost:5432/jetstream';
  const port = parseInt(process.env.PORT || '3001', 10);
  const host = process.env.HOST || '0.0.0.0';

  redis = new Redis(redisUrl);
  db = new pg.Pool({ connectionString: databaseUrl });

  await redis.ping();
  fastify.log.info('Connected to Redis');

  await db.query('SELECT 1');
  fastify.log.info('Connected to Postgres');

  await fastify.listen({ port, host });

  const shutdown = async () => {
    console.log('Shutting down...');
    await fastify.close();
    await redis.quit();
    await db.end();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch(console.error);
