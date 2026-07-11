// Lag and metric reporting. To keep the deployment package small (the
// applicationinsights npm package pulls in ~180 MB of OpenTelemetry), we do NOT
// use the App Insights SDK. Instead we emit metrics as structured log lines,
// which the Functions host forwards to Application Insights automatically. They
// can be aggregated in Kusto by parsing the JSON `metric`/`value` fields.
import { InvocationContext } from '@azure/functions';

export function trackLag(
    context: InvocationContext,
    name: string,
    value: number,
    properties?: Record<string, string>
): void {
    context.log(JSON.stringify({ evt: 'metric', metric: name, value, ...(properties ?? {}) }));
}
