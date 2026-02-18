/**
 * Enhanced DidoQ2 - Job Processing Logic
 * processJobBatch, fetchPendingJobs extracted from core-enhanced.ts
 */
import type { DidoQConfig, DidoQEnv, D1JobRecord } from './types';
/** Options for processJobBatch */
export interface BatchProcessOptions {
    env: DidoQEnv;
    config: DidoQConfig;
    shardId: number;
    ctx: DurableObjectState;
    processingJobs: Set<string>;
    processJobFn: (job: D1JobRecord) => Promise<{
        success: boolean;
        data?: unknown;
        error?: string;
    }>;
    markJobCompletedFn: (jobId: string, result: unknown) => Promise<void>;
    markJobFailedFn: (jobId: string, error: unknown) => Promise<void>;
}
/**
 * Process a batch of pending jobs.
 * Fetches, locks, processes, and handles results.
 */
export declare function processJobBatch(opts: BatchProcessOptions): Promise<void>;
/**
 * Fetch pending jobs with lock rescue mechanism.
 */
export declare function fetchPendingJobs(env: DidoQEnv, config: DidoQConfig, shardId: number, limit: number): Promise<D1JobRecord[]>;
