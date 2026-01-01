import pg from 'pg';
import type { OrderData } from '@jetstream-pg-writer/shared';
import { BaseHandler } from './base.js';

export class OrdersHandler extends BaseHandler<OrderData> {
  readonly table = 'orders';
  readonly consumerName = 'orders-writer';

  protected async insert(db: pg.Pool, operationId: string, data: OrderData): Promise<void> {
    await db.query(
      'INSERT INTO orders (id, user_id, items, total) VALUES ($1, $2, $3, $4)',
      [operationId, data.userId, JSON.stringify(data.items), data.total]
    );
  }
}
