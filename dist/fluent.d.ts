/**
 * DidoQ v2 Fluent API
 *
 * Chainable builder pattern for intuitive job queue operations.
 * Query builder methods are in fluent-query.ts.
 */
import type { Stream } from '@aid-on/nagare';
import type { DidoQConfig, DidoQEnv, JobProcessor } from './types';
import { FluentQueue } from './fluent-query';
export { FluentQueue };
/**
 * Job event emitted during processing lifecycle
 */
export interface JobEvent<T = unknown> {
    type: 'enqueued' | 'started' | 'processing' | 'completed' | 'failed' | 'heartbeat';
    jobId: string;
    timestamp: number;
    data?: T;
    error?: string;
}
/**
 * Fluent Job Builder
 * Chainable job creation and configuration
 */
export declare class FluentJob<TEnv extends DidoQEnv = DidoQEnv, TPayload = unknown> {
    private _jobId;
    private _userId?;
    private _jobType;
    private _payload?;
    private _config;
    private _env;
    private _processors;
    private _eventHandlers;
    constructor(env: TEnv, config?: Partial<DidoQConfig>);
    /** Set job ID */
    id(jobId: string): this;
    /** Set user ID */
    userId(userId: string): this;
    /** Set job type */
    type(jobType: string): this;
    /** Set payload */
    payload(payload: TPayload): this;
    /** Set shard ID */
    shard(_shardId: number): this;
    /** Add a job processor */
    process(processor: JobProcessor<TPayload>): this;
    /** Add an event handler */
    on(handler: (event: JobEvent<TPayload>) => void): this;
    /** Set timeout */
    timeout(ms: number): this;
    /** Set max retries */
    maxRetries(count: number): this;
    /** Enqueue the job */
    enqueue(): Promise<{
        jobId: string;
        shardId: number;
    }>;
    /** Process job as a nagare Stream */
    stream(): Stream<JobEvent<TPayload>>;
    /**
     * Handle a single poll result, emitting events as appropriate.
     * Returns true if the job has reached a terminal state.
     */
    private _handlePollResult;
    /** Get job status from D1 */
    private _getJobStatus;
    /** Emit event to all handlers */
    private _emitEvent;
}
/**
 * Fluent API entry point
 */
export declare class DidoQ<TEnv extends DidoQEnv = DidoQEnv> {
    private _env;
    private _config;
    constructor(env: TEnv, config?: Partial<DidoQConfig>);
    /** Create a new job */
    job<TPayload = unknown>(): FluentJob<TEnv, TPayload>;
    /** Get a queue manager */
    queue(): FluentQueue<TEnv>;
    /** Update configuration */
    configure(config: Partial<DidoQConfig>): this;
}
/**
 * Create a DidoQ instance
 */
export declare function didoq<TEnv extends DidoQEnv = DidoQEnv>(env: TEnv, config?: Partial<DidoQConfig>): DidoQ<TEnv>;
