/**
 * WebSocket Monitor Utilities
 * Sharding-aware utility functions
 */
/** Number of monitor shards */
export const MONITOR_SHARD_COUNT = 5;
/**
 * Compute a hash code for a string.
 * Same algorithm as core.ts getShardId.
 */
export function hashCode(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        hash = ((hash << 5) - hash) + str.charCodeAt(i);
        hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
}
/**
 * Get monitor shard ID from a job ID.
 */
export function getMonitorShardId(jobId, shardCount = MONITOR_SHARD_COUNT) {
    return Math.abs(hashCode(jobId)) % shardCount;
}
/**
 * Get a monitor DO stub for a given job ID.
 * Distributes jobs across a fixed number of monitor DOs via hashing.
 */
export function getJobMonitorStub(env, jobId) {
    const namespace = env.JOB_MONITOR || env.JOB_QUEUE;
    if (!namespace) {
        throw new Error('JOB_MONITOR namespace not found in environment');
    }
    const shardId = getMonitorShardId(jobId);
    const id = namespace.idFromName(`monitor-shard-${shardId}`);
    return namespace.get(id);
}
/**
 * Get a monitor DO stub for a given user ID.
 */
export function getUserMonitorStub(env, userId) {
    const namespace = env.JOB_MONITOR || env.JOB_QUEUE;
    if (!namespace) {
        throw new Error('JOB_MONITOR namespace not found in environment');
    }
    const id = namespace.idFromName(`monitor-user-${userId}`);
    return namespace.get(id);
}
/**
 * Get the global monitor DO stub (for admin usage).
 */
export function getGlobalMonitorStub(env) {
    const namespace = env.JOB_MONITOR || env.JOB_QUEUE;
    if (!namespace) {
        throw new Error('JOB_MONITOR namespace not found in environment');
    }
    const id = namespace.idFromName('monitor-global');
    return namespace.get(id);
}
