import { NatsConnection, StringCodec, JetStreamClient, AckPolicy, DeliverPolicy } from 'nats';
import type { Redis } from 'ioredis';
import type { Logger } from '@jetstream-pg-writer/shared/logger';
import { invalidateNamespace } from '@jetstream-pg-writer/shared/cache';

const sc = StringCodec();

// Debezium CDC event structure (after ExtractNewRecordState transform with delete.handling.mode=rewrite)
interface CdcEvent {
  // Record fields (column names match schema)
  user_id?: string;
  order_id?: string;
  // Metadata added by transform
  __op: 'c' | 'u' | 'd' | 'r'; // create, update, delete, read (snapshot)
  __table: string;
  __source_ts_ms: number;
  __deleted?: boolean; // Present on delete events with rewrite mode
}

interface CdcConsumerOptions {
  nc: NatsConnection;
  js: JetStreamClient;
  redis: Redis;
  log: Logger;
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
  // TRADEOFF: DeliverPolicy.All replays entire CDC history on first consumer creation.
  // This is acceptable because:
  // - Cache invalidation is idempotent (deleting a key twice is fine)
  // - Only happens on first deploy or if consumer is deleted
  // - Durable consumer tracks position after initial creation
  // - Write-processor does sync invalidation for its own writes anyway
  // - Cache has 30s TTL so stale data is bounded regardless
  //
  // Alternative: DeliverPolicy.New skips history but may miss events if consumer
  // is recreated while CDC events are in flight.
  try {
    await jsm.consumers.add(streamName, {
      durable_name: consumerName,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      filter_subjects: ['cdc.public.users', 'cdc.public.orders'],
      idle_heartbeat: 5_000_000_000, // 5s - detect stalled consumers
      flow_control: true, // Backpressure support for horizontal scaling
    });
    log.info({ consumer: consumerName }, 'Created CDC consumer');
  } catch (err: unknown) {
    if (err instanceof Error && !err.message.includes('already exists')) {
      throw err;
    }
    log.info({ consumer: consumerName }, 'CDC consumer already exists');
  }

  const consumer = await js.consumers.get(streamName, consumerName);
  return consumer.consume();
}

async function consumeCdcEvents(
  messages: AsyncIterable<import('nats').JsMsg>,
  redis: Redis,
  log: Logger
) {
  for await (const msg of messages) {
    try {
      const table = msg.subject.split('.').pop();
      const event: CdcEvent = JSON.parse(sc.decode(msg.data));

      const op = event.__op;

      log.info({ table, op }, 'CDC event received');

      // 'r' = snapshot read (Debezium initial sync) - ignore, not a real change
      if (op === 'r') {
        msg.ack();
        continue;
      }

      // Invalidate entire namespace - tracked keys make this O(n) where n = keys in namespace
      if (table === 'users') {
        await invalidateNamespace(redis, 'users');
        // User deletion may affect orders (FK relationship)
        if (op === 'd') {
          await invalidateNamespace(redis, 'orders');
          log.info({ userId: event.user_id, op }, 'Invalidated users + orders cache');
        } else {
          log.info({ userId: event.user_id, op }, 'Invalidated users cache');
        }
      } else if (table === 'orders') {
        await invalidateNamespace(redis, 'orders');
        log.info({ userId: event.user_id, op }, 'Invalidated orders cache');
      }

      msg.ack();
    } catch (err) {
      log.error({ err }, 'Failed to process CDC event');
      msg.nak(1000);
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
