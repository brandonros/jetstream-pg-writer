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

  protected async invalidateCache(operationId: string, _entityId: string, _data: OrderData): Promise<void> {
    const deleted = await this.invalidateNamespace('orders');
    this.log.info({ operationId, namespace: 'orders', keysDeleted: deleted }, 'Cache invalidated');
  }
}
