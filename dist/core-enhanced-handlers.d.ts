/**
 * Enhanced DidoQ2 - Job Handlers
 * processJob, processTextAnalysis, processPdfConversion,
 * markJobCompleted, markJobFailed extracted from core-enhanced.ts
 */
import type { DidoQConfig, DidoQEnv, D1JobRecord } from './types';
/** Context for job processing */
export interface JobProcessContext {
    env: DidoQEnv;
    config: DidoQConfig;
    shardId: number;
    processingJobs: Set<string>;
}
/**
 * Process an individual job.
 * Delegates to WORKER_SERVICE if available, otherwise handles inline.
 */
export declare function processJob(ctx: JobProcessContext, job: D1JobRecord): Promise<{
    success: boolean;
    data?: unknown;
    error?: string;
}>;
/**
 * Mark a job as completed in D1.
 */
export declare function markJobCompleted(env: DidoQEnv, config: DidoQConfig, jobId: string, result: unknown): Promise<void>;
/**
 * Mark a job as failed in D1.
 */
export declare function markJobFailed(env: DidoQEnv, config: DidoQConfig, jobId: string, error: unknown): Promise<void>;
