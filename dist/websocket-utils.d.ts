/**
 * WebSocket Monitor Utilities
 * Sharding-aware utility functions
 */
import type { DidoQEnv } from './types';
/** Number of monitor shards */
export declare const MONITOR_SHARD_COUNT = 5;
/**
 * Compute a hash code for a string.
 * Same algorithm as core.ts getShardId.
 */
export declare function hashCode(str: string): number;
/**
 * Get monitor shard ID from a job ID.
 */
export declare function getMonitorShardId(jobId: string, shardCount?: number): number;
/**
 * Get a monitor DO stub for a given job ID.
 * Distributes jobs across a fixed number of monitor DOs via hashing.
 */
export declare function getJobMonitorStub(env: DidoQEnv, jobId: string): DurableObjectStub;
/**
 * Get a monitor DO stub for a given user ID.
 */
export declare function getUserMonitorStub(env: DidoQEnv, userId: string): DurableObjectStub;
/**
 * Get the global monitor DO stub (for admin usage).
 */
export declare function getGlobalMonitorStub(env: DidoQEnv): DurableObjectStub;
