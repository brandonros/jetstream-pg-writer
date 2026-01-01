import { NatsConnection, StringCodec, JetStreamClient, AckPolicy, DeliverPolicy } from 'nats';
import type { Redis } from 'ioredis';
import type { FastifyBaseLogger } from 'fastify';

const sc = StringCodec();

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

interface CdcConsumerOptions {
  nc: NatsConnection;
  js: JetStreamClient;
  redis: Redis;
  log: FastifyBaseLogger;
}

export async function startCdcConsumer({ nc, js, redis, log }: CdcConsumerOptions) {
  // Wait for Debezium to create the stream (it auto-creates on startup)
  const streamName = 'DebeziumStream';
  const maxRetries = 30;
  const retryDelay = 2000;

  for (let i = 0; i < maxRetries; i++) {
    try {
      const jsm = await nc.jetstreamManager();
      await jsm.streams.info(streamName);
      log.info(`Found CDC stream: ${streamName}`);
      break;
    } catch (err) {
      if (i === maxRetries - 1) {
        log.error('CDC stream not found after max retries - cache invalidation disabled');
        return;
      }
      log.info(`Waiting for CDC stream (attempt ${i + 1}/${maxRetries})...`);
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
      deliver_policy: DeliverPolicy.New,
      filter_subjects: ['cdc.public.users', 'cdc.public.orders'],
    });
    log.info(`Created CDC consumer: ${consumerName}`);
  } catch (err: unknown) {
    if (err instanceof Error && !err.message.includes('already exists')) {
      log.warn({ err }, 'Error creating CDC consumer (may already exist)');
    }
  }

  const consumer = await js.consumers.get(streamName, consumerName);
  const messages = await consumer.consume();

  log.info('Listening for CDC events from Debezium');

  for await (const msg of messages) {
    try {
      const subject = msg.subject;
      const table = subject.split('.').pop();
      const event: CdcEvent = JSON.parse(sc.decode(msg.data));

      log.info({ table, op: event.__op }, 'CDC event received');

      if (table === 'users') {
        await redis.del('users:all');
        log.info('Invalidated users:all');
      } else if (table === 'orders') {
        await redis.del('orders:all');
        const userId = event.user_id;
        if (userId) {
          await redis.del(`orders:user:${userId}`);
          log.info({ userId }, 'Invalidated orders cache');
        }
      }

      msg.ack();
    } catch (err) {
      log.error({ err }, 'Failed to process CDC event');
      msg.nak();
    }
  }
}
