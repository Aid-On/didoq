/**
 * DidoQ v2 Fluent Query Builder
 * Extracted from fluent.ts - list, count, info, kickAll, stream, cleanup
 */
import type { Stream } from '@aid-on/nagare';
import type { DidoQConfig, DidoQEnv, JobStatus, D1JobRecord, QueueInfo } from './types';
import type { JobEvent } from './fluent';
/**
 * Fluent Queue Manager
 * Queue-wide management and monitoring
 */
export declare class FluentQueue<TEnv extends DidoQEnv = DidoQEnv> {
    private _env;
    private _config;
    private _filters;
    constructor(env: TEnv, config?: Partial<DidoQConfig>);
    /** Filter by status */
    withStatus(...statuses: JobStatus[]): this;
    /** Filter by user ID */
    forUser(userId: string): this;
    /** Filter by job type */
    ofType(jobType: string): this;
    /** Filter by shard ID */
    inShard(shardId: number): this;
    /** List jobs matching current filters */
    list(limit?: number): Promise<D1JobRecord[]>;
    /** Count jobs matching current filters */
    count(): Promise<number>;
    /** Get queue info from all shards */
    info(): Promise<QueueInfo[]>;
    /** Kick all shards to start processing */
    kickAll(): Promise<void>;
    /** Monitor jobs via nagare Stream */
    stream(): Stream<JobEvent>;
    /** Delete old completed/failed jobs */
    cleanup(olderThanMs?: number): Promise<number>;
}
