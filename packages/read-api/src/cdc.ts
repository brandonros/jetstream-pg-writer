import { NatsConnection, StringCodec, JetStreamClient, AckPolicy, DeliverPolicy } from 'nats';
import type { Redis } from 'ioredis';
import type { FastifyBaseLogger } from 'fastify';

const sc = StringCodec();

// Debezium CDC event structure (after ExtractNewRecordState transform)
interface CdcEvent {
  // Record fields (column names match new schema)
  user_id?: string;
  order_id?: string;
  // Metadata added by transform
  __op: 'c' | 'u' | 'd' | 'r'; // create, update, delete, read (snapshot)
  __table: string;
  __source_ts_ms: number;
  // For deletes, __deleted field is added
  __deleted?: string;
}

interface CdcConsumerOptions {
  nc: NatsConnection;
  js: JetStreamClient;
  redis: Redis;
  log: FastifyBaseLogger;
}

async function setupCdcConsumer({ nc, js, log }: Omit<CdcConsumerOptions, 'redis'>) {
  const streamName = 'DebeziumStream';
  const consumerName = 'cache-invalidator';
  const maxRetries = 30;
  const retryDelay = 2000;

  const jsm = await nc.jetstreamManager();

  // Wait for Debezium to create the stream
  let streamFound = false;
  for (let i = 0; i < maxRetries; i++) {
    try {
      await jsm.streams.info(streamName);
      log.info(`Found CDC stream: ${streamName}`);
      streamFound = true;
      break;
    } catch {
      log.info(`Waiting for CDC stream (attempt ${i + 1}/${maxRetries})...`);
      await new Promise((resolve) => setTimeout(resolve, retryDelay));
    }
  }

  if (!streamFound) {
    throw new Error('CDC stream not found after max retries');
  }

  // Create durable consumer
  try {
    await jsm.consumers.add(streamName, {
      durable_name: consumerName,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.New,
      filter_subjects: ['cdc.public.users', 'cdc.public.orders'],
    });
    log.info(`Created CDC consumer: ${consumerName}`);
  } catch (err: unknown) {
    if (err instanceof Error && !err.message.includes('already exists')) {
      throw err;
    }
  }

  const consumer = await js.consumers.get(streamName, consumerName);
  return consumer.consume();
}

async function consumeCdcEvents(
  messages: AsyncIterable<import('nats').JsMsg>,
  redis: Redis,
  log: FastifyBaseLogger
) {
  for await (const msg of messages) {
    try {
      const table = msg.subject.split('.').pop();
      const event: CdcEvent = JSON.parse(sc.decode(msg.data));

      log.info({ table, op: event.__op }, 'CDC event received');

      if (table === 'users') {
        await redis.del('users:all');
        log.info('Invalidated users:all');
      } else if (table === 'orders') {
        await redis.del('orders:all');
        if (event.user_id) {
          await redis.del(`orders:user:${event.user_id}`);
          log.info({ userId: event.user_id }, 'Invalidated orders cache');
        }
      }

      msg.ack();
    } catch (err) {
      log.error({ err }, 'Failed to process CDC event');
      msg.nak();
    }
  }
}

export async function startCdcConsumer({ nc, js, redis, log }: CdcConsumerOptions) {
  const messages = await setupCdcConsumer({ nc, js, log });
  log.info('CDC consumer ready, listening for events');

  // Run consumption loop - exit process on failure (let orchestrator restart)
  consumeCdcEvents(messages, redis, log).catch((err) => {
    log.error({ err }, 'CDC consumer loop failed, exiting');
    process.exit(1);
  });
}
