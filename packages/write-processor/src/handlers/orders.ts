import pg from 'pg';
import type { OrderData } from '@jetstream-pg-writer/shared';
import { BaseHandler } from './base.js';

export class OrdersHandler extends BaseHandler<OrderData> {
  readonly table = 'orders';
  readonly consumerName = 'orders-writer';

  protected async insert(client: pg.PoolClient, orderId: string, data: OrderData): Promise<void> {
    await client.query(
      'INSERT INTO orders (order_id, user_id, items, total) VALUES ($1, $2, $3, $4)',
      [orderId, data.userId, JSON.stringify(data.items), data.total]
    );
  }

  protected async invalidateCache(_entityId: string, data: OrderData): Promise<void> {
    await this.redis.del('orders:all');
    await this.redis.del(`orders:user:${data.userId}`);
  }
}
