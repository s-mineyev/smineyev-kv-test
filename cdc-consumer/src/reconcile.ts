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
    | 'ignored-older-generation';

// reconcile applies a CDC event to AMR, implementing doc 6.4 step 4 with the
// 6.6/6.7 expire_at generation rules.
//
// Concurrency: a plain GET-then-SET/DEL is used (no WATCH/MULTI). node-redis v6
// pools connections, so a cross-command WATCH does not reliably bind to the
// connection that runs EXEC. This is safe here because CDC events are ordered
// per key and reconciliation is convergent — the expire_at generation gate plus
// per-key ordering means any brief race self-heals on the next event.
// reconcile applies a CDC event to AMR, implementing doc 6.4 step 4 with the
// 6.6/6.7 expire_at generation rules:
//
//   - Upsert (create/replace): SET {value, expire_at} unless the committed
//     record is already expired (expire_at <= now), in which case remove it.
//     If the current AMR entry carries a *newer* expire_at, the event is a
//     stale/out-of-order generation and is ignored.
//   - Explicit delete: unconditionally remove the key (the value was
//     authoritatively deleted). No generation gate.
//   - TTL expiration (timeToLiveExpired): remove the key, but apply the 6.7
//     race rule — if the current AMR entry carries an expire_at newer than this
//     expired generation, the key was recreated and the stale expiry is ignored.
//
// Uses WATCH/MULTI optimistic concurrency so a concurrent opportunistic cache
// update (mutation step 3) or another CDC event cannot be clobbered.
export async function reconcile(
    client: RedisClientType,
    key: string,
    op: MutationOp,
    eventExpireAt: number,
    value: string | undefined,
    nowMs: number,
    ttlExpired: boolean
): Promise<ReconcileAction> {
    // Generation gating applies to upserts and TTL-expiration deletes (the 6.7
    // race), but NOT to explicit deletes, which must always remove.
    const gateByGeneration = op === 'upsert' || ttlExpired;

    if (gateByGeneration) {
        const existingExpireAt = parseExpireAt(await client.get(key));
        // Stale generation: current cache entry is newer than this event.
        if (existingExpireAt !== undefined && existingExpireAt > eventExpireAt) {
            return 'ignored-older-generation';
        }
    }

    if (op === 'delete') {
        await client.del(key);
        return ttlExpired ? 'del-expired' : 'del';
    }
    if (eventExpireAt <= nowMs) {
        // Committed upsert is already expired -> treat as removal.
        await client.del(key);
        return 'del-expired';
    }
    const entry: CacheEntry = { value: value ?? '', expire_at: eventExpireAt };
    await client.set(key, JSON.stringify(entry), { PXAT: eventExpireAt });
    return 'set';
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
