import { connect, NatsConnection, JetStreamClient, StringCodec, headers, createInbox } from 'nats';
import type { WriteRequest, WriteResponse, SupportedTable, TableDataMap } from '@jetstream-pg-writer/shared';
import type { Logger } from '@jetstream-pg-writer/shared/logger';

const sc = StringCodec();

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
}
