import { connect, NatsConnection, JetStreamClient, StringCodec, headers, createInbox } from 'nats';
import type { WriteRequest, WriteResponse, SupportedTable, TableDataMap } from '@jetstream-pg-writer/shared';
import type { Logger } from '@jetstream-pg-writer/shared/logger';

const sc = StringCodec();

/**
 * Publishes writes to JetStream with request-reply for confirmation.
 *
 * Why JetStream instead of NATS core request-reply?
 * - Durability: If the processor is down, writes queue in the stream and land when it recovers.
 *   With NATS core, writes during outages are lost unless the client retries.
 * - For high-value writes (orders, user signups), "eventually succeeds" beats "silently lost".
 * - Idempotency (operationId) ensures duplicate deliveries don't create duplicate records.
 *
 * Trade-off: More operational complexity (streams, consumers, ack policies) for write durability.
 */
export class WriteClient {
  private nc!: NatsConnection;
  private js!: JetStreamClient;
  private connected = false;
  private log: Logger;

  constructor(log: Logger) {
    this.log = log;
  }

  async connect(natsUrl = 'nats://localhost:4222') {
    if (this.connected) return;

    this.nc = await connect({ servers: natsUrl });
    this.js = this.nc.jetstream();

    this.connected = true;
    this.log.info({ natsUrl }, 'WriteClient connected');
  }

  async write<T extends SupportedTable>(
    table: T,
    data: TableDataMap[T],
    operationId: string,
    timeoutMs = 30000
  ): Promise<WriteResponse> {
    if (!this.connected) {
      throw new Error('WriteClient not connected');
    }

    const request: WriteRequest = {
      operationId,
      table,
      data: data as unknown as Record<string, unknown>,
    };

    const replySubject = createInbox();
    const sub = this.nc.subscribe(replySubject, { max: 1 });

    const h = headers();
    h.set('Reply-To', replySubject);

    await this.js.publish(`writes.${table}`, sc.encode(JSON.stringify(request)), {
      headers: h,
      msgID: operationId,
    });

    const timeout = setTimeout(() => sub.drain(), timeoutMs);

    for await (const msg of sub) {
      clearTimeout(timeout);
      return JSON.parse(sc.decode(msg.data)) as WriteResponse;
    }

    throw new Error(`Timeout waiting for write confirmation: ${operationId}`);
  }

  async close() {
    if (this.connected) {
      await this.nc.close();
      this.connected = false;
    }
  }

  isConnected() {
    return this.connected;
  }

  async healthCheck(): Promise<boolean> {
    if (!this.connected) return false;
    try {
      const jsm = await this.nc.jetstreamManager();
      await jsm.streams.info('WRITES');
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Returns pending message count in the WRITES stream.
   * Used for backpressure: reject new writes when queue is too deep.
   *
   * TRADEOFF: This is a point-in-time snapshot, not a reservation.
   * Under high concurrency, multiple requests may pass the check simultaneously
   * before any are processed. For true backpressure, use JetStream's
   * max_ack_pending on the consumer side. This is defense-in-depth.
   */
  async getQueueDepth(): Promise<number> {
    if (!this.connected) return 0;
    try {
      const jsm = await this.nc.jetstreamManager();
      const stream = await jsm.streams.info('WRITES');
      return stream.state.messages;
    } catch {
      return 0; // Can't check, assume healthy
    }
  }
}
