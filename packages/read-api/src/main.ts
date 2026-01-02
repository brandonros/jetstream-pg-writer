import Fastify from 'fastify';
import fastifyMetrics from 'fastify-metrics';
import pg from 'pg';
import { Redis } from 'ioredis';
import { connect } from 'nats';
import type { UserRow, OrderRow, OperationStatusResponse, OperationStatus, SupportedTable } from '@jetstream-pg-writer/shared';
import { createLogger } from '@jetstream-pg-writer/shared/logger';
import { setTrackedCache } from '@jetstream-pg-writer/shared/cache';
import { startCdcConsumer } from './cdc.js';

const log = createLogger('read-api');
const fastify = Fastify({ logger: log, trustProxy: true });

let db: pg.Pool;
let redis: Redis;

const CACHE_TTL_SECONDS = 30;

/**
 * Cache-aside pattern with Redis and tracked keys.
 *
 * TRADEOFF: Redis failure = read failure. This is intentional:
 * - Redis is a required infrastructure component, not an optimization
 * - Silently falling back to Postgres would mask infrastructure issues
 * - Health check already reports Redis status for alerting
 * - If Redis-optional behavior is needed, wrap calls in try/catch with metrics
 * NOTE: Assumes single Postgres instance. Read replicas would require
 * cache cooldown or primary reads on cache miss to avoid caching stale data.
 */
async function getCached<T>(
  namespace: 'users' | 'orders',
  key: string,
  fetcher: () => Promise<T>
): Promise<T> {
  const cached = await redis.get(key);
  if (cached) {
    log.info({ key }, 'Cache hit');
    return JSON.parse(cached);
  }

  log.info({ key }, 'Cache miss');
  const data = await fetcher();
  await setTrackedCache(redis, namespace, key, JSON.stringify(data), CACHE_TTL_SECONDS);
  return data;
}

fastify.get('/health', async (_, reply) => {
  const redisOk = redis.status === 'ready';
  let postgresOk = false;

  try {
    await db.query('SELECT 1');
    postgresOk = true;
  } catch {
    // postgres check failed
  }

  const status = redisOk && postgresOk ? 'ok' : 'degraded';
  const statusCode = status === 'ok' ? 200 : 503;

  return reply.status(statusCode).send({
    status,
    redis: redisOk,
    postgres: postgresOk,
  });
});

fastify.get<{ Querystring: { limit?: string; offset?: string } }>('/users', async (request) => {
  const limit = Math.min(parseInt(request.query.limit || '100', 10), 1000);
  const offset = parseInt(request.query.offset || '0', 10);
  const cacheKey = `users:limit:${limit}:offset:${offset}`;

  const users = await getCached<UserRow[]>('users', cacheKey, async () => {
    const result = await db.query<UserRow>(
      'SELECT user_id, name, email, created_at FROM users ORDER BY created_at DESC LIMIT $1 OFFSET $2',
      [limit, offset]
    );
    return result.rows;
  });

  return { users, limit, offset };
});

fastify.get<{ Querystring: { userId?: string; limit?: string; offset?: string } }>('/orders', async (request) => {
  const { userId } = request.query;
  const limit = Math.min(parseInt(request.query.limit || '100', 10), 1000);
  const offset = parseInt(request.query.offset || '0', 10);
  const cacheKey = userId
    ? `orders:user:${userId}:limit:${limit}:offset:${offset}`
    : `orders:limit:${limit}:offset:${offset}`;

  const orders = await getCached<OrderRow[]>('orders', cacheKey, async () => {
    if (userId) {
      const result = await db.query<OrderRow>(
        'SELECT order_id, user_id, items, total, created_at FROM orders WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3',
        [userId, limit, offset]
      );
      return result.rows;
    }

    const result = await db.query<OrderRow>(
      'SELECT order_id, user_id, items, total, created_at FROM orders ORDER BY created_at DESC LIMIT $1 OFFSET $2',
      [limit, offset]
    );
    return result.rows;
  });

  return { orders, limit, offset };
});

// Operation status polling endpoint (for async writes)
interface WriteOperationRow {
  status: OperationStatus;
  entity_table: SupportedTable;
  entity_id: string;
  error: string | null;
}

fastify.get<{ Params: { operationId: string } }>('/status/:operationId', async (request): Promise<OperationStatusResponse> => {
  const { operationId } = request.params;

  const result = await db.query<WriteOperationRow>(
    'SELECT status, entity_table, entity_id, error FROM write_operations WHERE operation_id = $1',
    [operationId]
  );

  if (result.rows.length === 0) {
    // Not found = still pending (not yet processed)
    return { status: 'pending', operationId };
  }

  const row = result.rows[0];
  return {
    status: row.status,
    operationId,
    table: row.entity_table,
    entityId: row.entity_id,
    ...(row.error && { error: row.error }),
  };
});

async function main() {
  const natsUrl = process.env.NATS_URL || 'nats://localhost:4222';
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  const databaseUrl = process.env.DATABASE_URL || 'postgres://jetstream:jetstream@localhost:5432/jetstream';
  const port = parseInt(process.env.PORT || '3001', 10);
  const host = process.env.HOST || '0.0.0.0';

  await fastify.register(fastifyMetrics.default, {
    endpoint: '/metrics',
    defaultMetrics: { enabled: true },
    routeMetrics: { enabled: true },
  });

  const nc = await connect({ servers: natsUrl });
  log.info('Connected to NATS');

  const js = nc.jetstream();

  redis = new Redis(redisUrl);
  db = new pg.Pool({ connectionString: databaseUrl });

  await redis.ping();
  log.info('Connected to Redis');

  await db.query('SELECT 1');
  log.info('Connected to Postgres');

  // Start CDC consumer for cache invalidation (Debezium → JetStream)
  await startCdcConsumer({ nc, js, redis, log });

  await fastify.listen({ port, host });

  const shutdown = async () => {
    log.info('Shutting down...');
    await fastify.close();
    await nc.close();
    await redis.quit();
    await db.end();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  log.error({ err }, 'Fatal error');
  process.exit(1);
});
