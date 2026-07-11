import { app, InvocationContext } from '@azure/functions';
import { getAmrClient } from '../amr';
import { reconcile, MutationOp, ReconcileAction } from '../reconcile';
import { trackLag } from '../telemetry';

// Cosmos DB document shape written by the Go mutator.
interface KvDoc {
    id?: string;
    value?: string;
    expire_at?: number;
    written_at_ms?: number;
    ttl?: number;
    _ts?: number;
}

// A single change-feed item in "AllVersionsAndDeletes" (full-fidelity) mode.
interface FullFidelityChange {
    current?: KvDoc;
    previous?: KvDoc;
    metadata?: {
        operationType?: 'create' | 'replace' | 'delete';
        crts?: number; // commit timestamp (seconds)
        timeToLiveExpired?: boolean;
    };
}

interface ParsedEvent {
    key: string;
    op: MutationOp;
    value?: string;
    eventExpireAt: number;
    writtenAtMs?: number;
    commitTsSec?: number;
    ttlExpired: boolean;
}

// cdcReconcile consumes the Cosmos DB change feed for kvdb/kvcache in
// AllVersionsAndDeletes mode and reconciles AMR toward committed durable state
// (doc 6.4 step 4). Change events are ordered per key, so AMR converges without
// any additional version metadata beyond expire_at.
export async function cdcReconcile(documents: unknown[], context: InvocationContext): Promise<void> {
    const batchSize = Array.isArray(documents) ? documents.length : 0;
    const invocationStart = Date.now();
    context.log(JSON.stringify({ evt: 'cdc_invocation_start', batchSize, invocationId: context.invocationId }));

    if (batchSize === 0) {
        return;
    }

    const client = await getAmrClient();

    for (const raw of documents) {
        const parsed = parseChange(raw);
        if (!parsed) {
            context.warn(JSON.stringify({ evt: 'cdc_skip_unparsed', raw }));
            continue;
        }

        const nowMs = Date.now();
        let action: ReconcileAction;
        try {
            action = await reconcile(client, parsed.key, parsed.op, parsed.eventExpireAt, parsed.value, nowMs);
        } catch (err) {
            context.error(JSON.stringify({ evt: 'cdc_reconcile_error', key: parsed.key, op: parsed.op, error: `${(err as Error)?.message ?? err}` }));
            throw err; // let the trigger retry the batch
        }

        // Lag measurement (doc: CDC is eventually consistent).
        const lagMs = parsed.writtenAtMs !== undefined ? nowMs - parsed.writtenAtMs : undefined;
        const commitLagMs = parsed.commitTsSec !== undefined ? nowMs - parsed.commitTsSec * 1000 : undefined;

        if (lagMs !== undefined) {
            trackLag('cdc_lag_ms', lagMs, { op: parsed.op, action });
        }
        if (commitLagMs !== undefined) {
            trackLag('cdc_commit_lag_ms', commitLagMs, { op: parsed.op, action });
        }

        context.log(JSON.stringify({
            evt: 'cdc_reconcile',
            key: parsed.key,
            op: parsed.op,
            ttlExpired: parsed.ttlExpired,
            action,
            expire_at: parsed.eventExpireAt,
            lag_ms: lagMs,
            commit_lag_ms: commitLagMs,
        }));
    }

    context.log(JSON.stringify({
        evt: 'cdc_invocation_end',
        batchSize,
        durationMs: Date.now() - invocationStart,
        invocationId: context.invocationId,
    }));
}

// parseChange normalizes a change-feed item (full-fidelity or latest-version)
// into a ParsedEvent, or returns undefined if it can't be interpreted.
function parseChange(raw: unknown): ParsedEvent | undefined {
    if (!raw || typeof raw !== 'object') {
        return undefined;
    }
    const ff = raw as FullFidelityChange;

    // Full-fidelity (AllVersionsAndDeletes) shape.
    if (ff.metadata && ff.metadata.operationType) {
        const opType = ff.metadata.operationType;
        const commitTsSec = ff.metadata.crts;
        const ttlExpired = ff.metadata.timeToLiveExpired === true;

        if (opType === 'delete') {
            const doc = ff.previous ?? ff.current ?? {};
            const key = doc.id ?? ff.current?.id;
            if (!key) {
                return undefined;
            }
            // Use the deleted record's expire_at as the generation; fall back to
            // the commit timestamp so a later recreation is not clobbered.
            const eventExpireAt = doc.expire_at ?? (commitTsSec ? commitTsSec * 1000 : Date.now());
            return { key, op: 'delete', eventExpireAt, commitTsSec, ttlExpired };
        }

        // create / replace
        const doc = ff.current;
        if (!doc?.id) {
            return undefined;
        }
        return {
            key: doc.id,
            op: 'upsert',
            value: doc.value,
            eventExpireAt: doc.expire_at ?? Number.MAX_SAFE_INTEGER,
            writtenAtMs: doc.written_at_ms,
            commitTsSec,
            ttlExpired: false,
        };
    }

    // Latest-version fallback (plain document, upsert only).
    const doc = raw as KvDoc;
    if (!doc.id) {
        return undefined;
    }
    return {
        key: doc.id,
        op: 'upsert',
        value: doc.value,
        eventExpireAt: doc.expire_at ?? Number.MAX_SAFE_INTEGER,
        writtenAtMs: doc.written_at_ms,
        commitTsSec: doc._ts,
        ttlExpired: false,
    };
}

app.cosmosDB('cdcReconcile', {
    connection: 'COSMOS',
    databaseName: process.env.COSMOS_DB ?? 'kvdb',
    containerName: process.env.COSMOS_CONTAINER ?? 'kvcache',
    leaseContainerName: 'leases',
    createLeaseContainerIfNotExists: true,
    // Full-fidelity change feed: required to propagate deletes and TTL
    // expirations (doc 4.4). Not in the typed options yet, so passed through.
    changeFeedMode: 'AllVersionsAndDeletes',
    startFromBeginning: false,
    handler: cdcReconcile,
} as any);
