import type { Redis } from 'ioredis';

/**
 * Delete all Redis keys matching a pattern (e.g., 'users:*').
 * Uses SCAN to avoid blocking Redis on large keyspaces.
 */
export async function deleteByPattern(redis: Redis, pattern: string): Promise<number> {
  let deleted = 0;
  let cursor = '0';

  do {
    const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
    cursor = nextCursor;
    if (keys.length > 0) {
      deleted += await redis.del(...keys);
    }
  } while (cursor !== '0');

  return deleted;
}
