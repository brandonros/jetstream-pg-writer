import type { Redis } from 'ioredis';

// Cache key tracking sets - avoids SCAN which iterates entire keyspace
const CACHE_KEYS_SET = {
  users: 'cache:keys:users',
  orders: 'cache:keys:orders',
} as const;

type CacheNamespace = keyof typeof CACHE_KEYS_SET;

/**
 * Set a cache value and track the key for efficient invalidation.
 * Uses a Redis Set to track keys per namespace instead of SCAN.
 */
export async function setTrackedCache(
  redis: Redis,
  namespace: CacheNamespace,
  key: string,
  value: string,
  ttlSeconds: number
): Promise<void> {
  await redis.pipeline()
    .setex(key, ttlSeconds, value)
    .sadd(CACHE_KEYS_SET[namespace], key)
    .exec();
}

/**
 * Invalidate all cached keys for a namespace.
 * O(n) where n = number of keys in namespace, not entire keyspace.
 */
export async function invalidateNamespace(redis: Redis, namespace: CacheNamespace): Promise<number> {
  const setKey = CACHE_KEYS_SET[namespace];
  const keys = await redis.smembers(setKey);

  if (keys.length === 0) {
    return 0;
  }

  // Delete all tracked keys + the tracking set
  const deleted = await redis.del(...keys, setKey);
  return deleted - 1; // Don't count the set itself
}

/**
 * @deprecated Use invalidateNamespace instead - SCAN iterates entire keyspace
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
