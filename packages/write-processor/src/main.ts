import { connect, AckPolicy } from 'nats';
import pg from 'pg';
import { Redis } from 'ioredis';
import { UsersHandler } from './handlers/users.js';
import { OrdersHandler } from './handlers/orders.js';

const { Pool } = pg;

const HANDLERS = [
  { name: 'users-writer', filterSubject: 'writes.users' },
  { name: 'orders-writer', filterSubject: 'writes.orders' },
];

async function main() {
  const natsUrl = process.env.NATS_URL || 'nats://localhost:4222';
  const databaseUrl = process.env.DATABASE_URL || 'postgres://jetstream:jetstream@localhost:5432/jetstream';
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

  const db = new Pool({ connectionString: databaseUrl });
  const redis = new Redis(redisUrl);
  const nc = await connect({ servers: natsUrl });
  const js = nc.jetstream();
  const jsm = await nc.jetstreamManager();

  console.log(`Connected to ${natsUrl}`);

  // Ensure streams exist
  try {
    await jsm.streams.add({
      name: 'WRITES',
      subjects: ['writes.>'],
    });
    console.log('Created WRITES stream');
  } catch {
    console.log('WRITES stream already exists');
  }

  // Create filtered consumers for each handler
  for (const { name, filterSubject } of HANDLERS) {
    try {
      await jsm.consumers.add('WRITES', {
        durable_name: name,
        ack_policy: AckPolicy.Explicit,
        filter_subject: filterSubject,
        max_deliver: 3,
        ack_wait: 30_000_000_000,
      });
      console.log(`Created consumer: ${name} (${filterSubject})`);
    } catch {
      console.log(`Consumer already exists: ${name}`);
    }
  }

  // Instantiate handlers
  const handlers = [
    new UsersHandler(nc, js, db, redis),
    new OrdersHandler(nc, js, db, redis),
  ];

  // Graceful shutdown
  const shutdown = async () => {
    console.log('Shutting down...');
    await nc.close();
    await db.end();
    await redis.quit();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  // Start all handlers concurrently
  console.log('Starting handlers...');
  await Promise.all(handlers.map(h => h.start()));
}

main().catch(console.error);
