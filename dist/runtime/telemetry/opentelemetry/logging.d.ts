import { type BatchLogRecordProcessorOptions, type LogRecordExporter } from "@opentelemetry/sdk-logs";
import type { Context } from "../../context.js";
import { type LogConfig, type Logger, type LogsEngine } from "../../environment/index.js";
export interface OpenTelemetryLogsOptions {
    readonly serviceName: string;
    readonly endpoint?: string;
    readonly exportTimeoutMillis?: number;
    readonly exporter?: LogRecordExporter;
    readonly batch?: Omit<BatchLogRecordProcessorOptions, "exporter">;
    readonly resourceAttributes?: Readonly<Record<string, string | number | boolean>>;
    readonly now?: () => Date;
}
export declare class OpenTelemetryLogsEngine implements LogsEngine {
    #private;
    constructor(options: OpenTelemetryLogsOptions);
    defaultLogger(config?: LogConfig): Logger;
    shutdown(context: Context): Promise<void>;
}
//# sourceMappingURL=logging.d.ts.map