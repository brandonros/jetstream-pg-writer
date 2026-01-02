import pg from 'pg';
import type { UserData } from '@jetstream-pg-writer/shared';
import { BaseHandler } from './base.js';

export class UsersHandler extends BaseHandler<UserData> {
  readonly table = 'users';
  readonly consumerName = 'users-writer';

  protected async insert(client: pg.PoolClient, userId: string, data: UserData): Promise<void> {
    await client.query(
      'INSERT INTO users (user_id, name, email) VALUES ($1, $2, $3)',
      [userId, data.name, data.email]
    );
  }

  protected async invalidateCache(operationId: string, _entityId: string, _data: UserData): Promise<void> {
    const deleted = await this.invalidateNamespace('users');
    this.log.info({ operationId, namespace: 'users', keysDeleted: deleted }, 'Cache invalidated');
  }
}
