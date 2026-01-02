import Fastify from 'fastify';
import rateLimit from '@fastify/rate-limit';
import { serializerCompiler, validatorCompiler, ZodTypeProvider } from 'fastify-type-provider-zod';
import { createLogger } from '@jetstream-pg-writer/shared/logger';
import { WriteClient, BackpressureError, CircuitOpenError } from './client.js';
import { UserDataSchema, OrderDataSchema } from '@jetstream-pg-writer/shared';

const log = createLogger('write-gateway');
const fastify = Fastify({ logger: log, trustProxy: true });

fastify.setValidatorCompiler(validatorCompiler);
fastify.setSerializerCompiler(serializerCompiler);

const app = fastify.withTypeProvider<ZodTypeProvider>();
const writeClient = new WriteClient(log);

// Health check with backpressure stats
app.get('/health', async (_request, reply) => {
  const healthy = await writeClient.healthCheck();
  const stats = writeClient.getStats();

  if (healthy && !stats.circuitOpen) {
    return { status: 'ok', ...stats };
  }
  return reply.status(503).send({ status: 'unhealthy', ...stats });
});

// Create user (async - returns queued, client polls for completion)
app.post('/users', {
  schema: { body: UserDataSchema },
}, async (request, reply) => {
  const idempotencyKey = request.headers['idempotency-key'];
  if (!idempotencyKey || typeof idempotencyKey !== 'string') {
    return reply.status(400).send({ error: 'Idempotency-Key header is required' });
  }

  const { name, email } = request.body;

  try {
    await writeClient.write('users', { name, email }, idempotencyKey);
  } catch (error) {
    if (error instanceof BackpressureError || error instanceof CircuitOpenError) {
      return reply
        .status(503)
        .header('Retry-After', '5')
        .send({ error: error.message });
    }
    throw error;
  }

  return reply.status(202).send({
    status: 'queued',
    operationId: idempotencyKey,
    acceptedAt: new Date().toISOString(),
  });
});

// Create order (async - returns queued, client polls for completion)
app.post('/orders', {
  schema: { body: OrderDataSchema },
}, async (request, reply) => {
  const idempotencyKey = request.headers['idempotency-key'];
  if (!idempotencyKey || typeof idempotencyKey !== 'string') {
    return reply.status(400).send({ error: 'Idempotency-Key header is required' });
  }

  const { userId, items, total } = request.body;

  try {
    await writeClient.write('orders', { userId, items, total }, idempotencyKey);
  } catch (error) {
    if (error instanceof BackpressureError || error instanceof CircuitOpenError) {
      return reply
        .status(503)
        .header('Retry-After', '5')
        .send({ error: error.message });
    }
    throw error;
  }

  return reply.status(202).send({
    status: 'queued',
    operationId: idempotencyKey,
    acceptedAt: new Date().toISOString(),
  });
});

async function main() {
  const natsUrl = process.env.NATS_URL || 'nats://localhost:4222';
  const port = parseInt(process.env.PORT || '3000', 10);
  const host = process.env.HOST || '0.0.0.0';

  await fastify.register(rateLimit, {
    max: 100,
    timeWindow: '1 minute',
  });

  await writeClient.connect(natsUrl);

  await fastify.listen({ port, host });

  const shutdown = async () => {
    log.info('Shutting down...');
    await fastify.close();
    await writeClient.close();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  log.error({ err }, 'Fatal error');
  process.exit(1);
});
