import { connect, NatsConnection, JetStreamClient, StringCodec } from 'nats';
import type { WriteRequest, SupportedTable, TableDataMap } from '@jetstream-pg-writer/shared';
import type { Logger } from '@jetstream-pg-writer/shared/logger';

const sc = StringCodec();

/**
 * Publishes writes to JetStream (fire-and-forget).
 *
 * Async pattern: Gateway returns immediately after JetStream ack.
 * Client polls /status/:operationId to check completion.
 *
 * Benefits:
 * - Durability: If processor is down, writes queue and complete when it recovers
 * - No timeout: Client can poll for as long as needed
 * - Idempotency: operationId ensures duplicate deliveries don't create duplicate records
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
    operationId: string
  ): Promise<void> {
    if (!this.connected) {
      throw new Error('WriteClient not connected');
    }

    const request: WriteRequest = {
      operationId,
      table,
      data: data as unknown as Record<string, unknown>,
    };

    await this.js.publish(`writes.${table}`, sc.encode(JSON.stringify(request)), {
      msgID: operationId,
    });

    this.log.info({ table, operationId }, 'Write published to JetStream');
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
}
