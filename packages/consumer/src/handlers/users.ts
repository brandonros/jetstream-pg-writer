import pg from 'pg';
import type { UserData } from '@jetstream-pg-writer/shared';
import { BaseHandler } from './base.js';

export class UsersHandler extends BaseHandler<UserData> {
  readonly table = 'users';
  readonly consumerName = 'users-writer';

  protected async insert(db: pg.Pool, operationId: string, data: UserData): Promise<void> {
    await db.query(
      'INSERT INTO users (id, name, email) VALUES ($1, $2, $3)',
      [operationId, data.name, data.email]
    );
  }
}
