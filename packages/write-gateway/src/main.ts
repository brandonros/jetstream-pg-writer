import Fastify from 'fastify';
import rateLimit from '@fastify/rate-limit';
import { serializerCompiler, validatorCompiler, ZodTypeProvider } from 'fastify-type-provider-zod';
import { createLogger } from '@jetstream-pg-writer/shared/logger';
import { WriteClient } from './client.js';
import { UserDataSchema, OrderDataSchema } from '@jetstream-pg-writer/shared';

const log = createLogger('write-gateway');
const fastify = Fastify({ logger: log, trustProxy: true });

fastify.setValidatorCompiler(validatorCompiler);
fastify.setSerializerCompiler(serializerCompiler);

const app = fastify.withTypeProvider<ZodTypeProvider>();
const writeClient = new WriteClient(log);

// Health check
app.get('/health', async (_request, reply) => {
  const healthy = await writeClient.healthCheck();
  if (healthy) {
    return { status: 'ok' };
  }
  return reply.status(503).send({ status: 'unhealthy' });
});

// Create user
app.post('/users', {
  schema: { body: UserDataSchema },
}, async (request, reply) => {
  const idempotencyKey = request.headers['idempotency-key'];
  if (!idempotencyKey || typeof idempotencyKey !== 'string') {
    return reply.status(400).send({ error: 'Idempotency-Key header is required' });
  }

  const { name, email } = request.body;
  const result = await writeClient.write('users', { name, email }, idempotencyKey);

  if (result.success) {
    return reply.status(201).send({
      userId: result.entityId,
    });
  }

  return reply.status(500).send({
    error: result.error,
  });
});

// Create order
app.post('/orders', {
  schema: { body: OrderDataSchema },
}, async (request, reply) => {
  const idempotencyKey = request.headers['idempotency-key'];
  if (!idempotencyKey || typeof idempotencyKey !== 'string') {
    return reply.status(400).send({ error: 'Idempotency-Key header is required' });
  }

  const { userId, items, total } = request.body;
  const result = await writeClient.write('orders', { userId, items, total }, idempotencyKey);

  if (result.success) {
    return reply.status(201).send({
      orderId: result.entityId,
    });
  }

  return reply.status(500).send({
    error: result.error,
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
