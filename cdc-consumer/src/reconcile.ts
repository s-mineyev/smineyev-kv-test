import { RedisClientType } from 'redis';

// cacheEntry is the JSON value stored in AMR for a key. It must stay in sync
// with the Go writer's cacheEntry ({value, expire_at}).
export interface CacheEntry {
    value: string;
    expire_at: number;
}

export type MutationOp = 'upsert' | 'delete';

export type ReconcileAction =
    | 'set'
    | 'del'
    | 'del-expired'
    | 'ignored-older-generation'
    | 'conflict-retries-exhausted';

const MAX_RETRIES = 5;

// reconcile applies a CDC event to AMR, implementing doc 6.4 step 4 with the
// 6.6/6.7 expire_at generation rules:
//
//   - Upsert (create/replace): SET {value, expire_at} unless the committed
//     record is already expired (expire_at <= now), in which case remove it.
//   - Delete / TTL expiration: remove the key.
//   - In all cases, if the current AMR entry carries a *newer* expire_at than
//     this event's generation, the event is stale (the key was recreated) and
//     is ignored. Equal or older generations proceed.
//
// Uses WATCH/MULTI optimistic concurrency so a concurrent opportunistic cache
// update (mutation step 3) or another CDC event cannot be clobbered.
export async function reconcile(
    client: RedisClientType,
    key: string,
    op: MutationOp,
    eventExpireAt: number,
    value: string | undefined,
    nowMs: number
): Promise<ReconcileAction> {
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        await client.watch(key);

        const raw = await client.get(key);
        const existingExpireAt = parseExpireAt(raw);

        // Stale generation: current cache entry is newer than this event.
        if (existingExpireAt !== undefined && existingExpireAt > eventExpireAt) {
            await client.unwatch();
            return 'ignored-older-generation';
        }

        const multi = client.multi();
        let action: ReconcileAction;

        if (op === 'delete' || eventExpireAt <= nowMs) {
            multi.del(key);
            action = op === 'delete' ? 'del' : 'del-expired';
        } else {
            const entry: CacheEntry = { value: value ?? '', expire_at: eventExpireAt };
            multi.set(key, JSON.stringify(entry), { PXAT: eventExpireAt });
            action = 'set';
        }

        const result = await multi.exec();
        // exec() returns null when a watched key changed; retry.
        if (result !== null) {
            return action;
        }
    }
    return 'conflict-retries-exhausted';
}

function parseExpireAt(raw: unknown): number | undefined {
    if (typeof raw !== 'string' || raw.length === 0) {
        return undefined;
    }
    try {
        const entry = JSON.parse(raw) as Partial<CacheEntry>;
        return typeof entry.expire_at === 'number' ? entry.expire_at : undefined;
    } catch {
        return undefined;
    }
}
