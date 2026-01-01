import { connect, NatsConnection, JetStreamClient, StringCodec } from 'nats';
import { WriteRequest, WriteResponse } from '../shared/types';

const sc = StringCodec();

class Producer {
  private nc!: NatsConnection;
  private js!: JetStreamClient;

  async connect() {
    const natsUrl = process.env.NATS_URL || 'nats://localhost:4222';
    
    this.nc = await connect({ servers: natsUrl });
    this.js = this.nc.jetstream();

    // Ensure stream exists
    const jsm = await this.nc.jetstreamManager();
    try {
      await jsm.streams.add({
        name: 'WRITES',
        subjects: ['writes.>'],
      });
    } catch (e) {
      // Stream already exists, that's fine
    }
    
    console.log(`Producer connected to ${natsUrl}`);
  }

  async queueWrite(request: WriteRequest, timeoutMs = 30000): Promise<WriteResponse> {
    const replySubject = this.nc.newInbox();
    const sub = this.nc.subscribe(replySubject, { max: 1 });

    await this.js.publish(`writes.${request.table}`, sc.encode(JSON.stringify(request)), {
      headers: new Map([['Reply-To', replySubject]]) as any,
      msgID: request.operationId,
    });

    const timeout = setTimeout(() => sub.drain(), timeoutMs);
    
    for await (const msg of sub) {
      clearTimeout(timeout);
      return JSON.parse(sc.decode(msg.data)) as WriteResponse;
    }

    throw new Error(`Timeout waiting for write confirmation: ${request.operationId}`);
  }

  async close() {
    await this.nc.close();
  }
}

async function main() {
  const producer = new Producer();
  await producer.connect();

  const result = await producer.queueWrite({
    operationId: crypto.randomUUID(),
    table: 'users',
    data: { name: 'Alice', email: 'alice@example.com' },
  });

  console.log('Write result:', result);
  await producer.close();
}

main().catch(console.error);