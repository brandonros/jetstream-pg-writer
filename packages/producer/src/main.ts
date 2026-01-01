import Fastify from 'fastify';
import { WriteClient } from './client.js';
import type { UserData, OrderData } from '@jetstream-pg-writer/shared';

const fastify = Fastify({ logger: true });
const writeClient = new WriteClient();

// Health check
fastify.get('/health', async () => {
  return {
    status: 'ok',
    nats: writeClient.isConnected(),
  };
});

// Create user
fastify.post<{ Body: UserData }>('/users', async (request, reply) => {
  const { name, email } = request.body;

  if (!name || !email) {
    return reply.status(400).send({ error: 'name and email are required' });
  }

  const result = await writeClient.write('users', { name, email });

  if (result.success) {
    return reply.status(201).send({
      operationId: result.operationId,
      message: 'User created',
    });
  }

  return reply.status(500).send({
    error: result.error,
    operationId: result.operationId,
  });
});

// Create order
fastify.post<{ Body: OrderData }>('/orders', async (request, reply) => {
  const { userId, items, total } = request.body;

  if (!userId || !items || total === undefined) {
    return reply.status(400).send({ error: 'userId, items, and total are required' });
  }

  const result = await writeClient.write('orders', { userId, items, total });

  if (result.success) {
    return reply.status(201).send({
      operationId: result.operationId,
      message: 'Order created',
    });
  }

  return reply.status(500).send({
    error: result.error,
    operationId: result.operationId,
  });
});

async function main() {
  const natsUrl = process.env.NATS_URL || 'nats://localhost:4222';
  const port = parseInt(process.env.PORT || '3000', 10);
  const host = process.env.HOST || '0.0.0.0';

  await writeClient.connect(natsUrl);

  await fastify.listen({ port, host });

  const shutdown = async () => {
    console.log('Shutting down...');
    await fastify.close();
    await writeClient.close();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch(console.error);
