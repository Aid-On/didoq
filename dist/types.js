/**
 * DidoQ v2 - Type definitions
 * Independent from didoq v1
 */
/**
 * デフォルト設定
 */
export const DEFAULT_CONFIG = {
    tableName: 'jobs',
    shardCount: 10,
    timeoutMs: 3 * 60 * 1000,
    heartbeatIntervalMs: 30 * 1000,
    alarmCheckIntervalMs: 5000,
    maxRetryCount: 3,
    nextJobDelayMs: 100,
    websocketEnabled: true,
    flushInterval: 5000,
    batchSize: 100,
    maxRetries: 3,
};
