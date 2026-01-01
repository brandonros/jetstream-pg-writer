import pg from 'pg';
import type { UserData } from '@jetstream-pg-writer/shared';
import { BaseHandler } from './base.js';

export class UsersHandler extends BaseHandler<UserData> {
  readonly table = 'users';
  readonly consumerName = 'users-writer';

  protected async insert(client: pg.PoolClient, _operationId: string, data: UserData): Promise<void> {
    await client.query(
      'INSERT INTO users (name, email) VALUES ($1, $2)',
      [data.name, data.email]
    );
  }
}
