import { createClient, RedisClientType } from 'redis';
import { EntraIdCredentialsProviderFactory, REDIS_SCOPE_DEFAULT } from '@redis/entraid';
import { DefaultAzureCredential } from '@azure/identity';

// AMR (Azure Managed Redis) connection details. Access-key auth is disabled on
// the cluster, so we authenticate with Microsoft Entra ID. The @redis/entraid
// provider acquires tokens (scope https://redis.azure.com/.default), maps the
// principal's object ID to the Redis username, and refreshes automatically.
const AMR_HOST = process.env.AMR_HOST ?? 'amr1-test1.centralus.redis.azure.net';
const AMR_PORT = Number(process.env.AMR_PORT ?? '10000');

let clientPromise: Promise<RedisClientType> | undefined;

// getAmrClient returns a lazily-created, shared, connected Redis client. Using a
// singleton keeps one authenticated TLS connection warm across invocations.
export function getAmrClient(): Promise<RedisClientType> {
    if (!clientPromise) {
        clientPromise = connect();
    }
    return clientPromise;
}

async function connect(): Promise<RedisClientType> {
    const credentialsProvider = EntraIdCredentialsProviderFactory.createForDefaultAzureCredential({
        credential: new DefaultAzureCredential(),
        scopes: REDIS_SCOPE_DEFAULT,
        tokenManagerConfig: {
            // Refresh well before expiry so the long-running function keeps a valid token.
            expirationRefreshRatio: 0.8,
        },
    });

    const client: RedisClientType = createClient({
        url: `rediss://${AMR_HOST}:${AMR_PORT}`,
        credentialsProvider,
        socket: {
            tls: true,
            // SNI / cert validation host.
            servername: AMR_HOST,
            reconnectStrategy: (retries) => Math.min(retries * 100, 3000),
        },
    });

    client.on('error', (err) => {
        // Surface connection/auth errors to the Functions logs.
        console.error(`[amr] client error: ${err?.message ?? err}`);
    });

    await client.connect();
    return client;
}
