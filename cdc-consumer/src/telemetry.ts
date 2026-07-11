import { TelemetryClient } from 'applicationinsights';

// A standalone TelemetryClient (not appInsights.setup(), which would conflict
// with the Functions host's auto-instrumentation) used only to emit custom
// metrics such as cdc_lag_ms. No-ops when no connection string is configured.
let client: TelemetryClient | undefined;
let initialized = false;

function getClient(): TelemetryClient | undefined {
    if (!initialized) {
        initialized = true;
        const conn = process.env.APPLICATIONINSIGHTS_CONNECTION_STRING;
        if (conn) {
            client = new TelemetryClient(conn);
        }
    }
    return client;
}

export function trackLag(name: string, value: number, properties?: Record<string, string>): void {
    const c = getClient();
    if (c) {
        c.trackMetric({ name, value, properties });
    }
}
