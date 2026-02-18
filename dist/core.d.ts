/**
 * DidoQ v2 - Core functionality
 * Independent implementation without didoq v1 dependency
 */
import type { DidoQConfig, DidoQEnv, JobEnqueueRequest, JobCompleteRequest, AggregatedQueueInfo } from './types';
import { DEFAULT_CONFIG } from './types';
export { DEFAULT_CONFIG };
/**
 * シャードIDを計算
 */
export declare function getShardId(jobId: string, shardCount: number): number;
/**
 * Durable Object スタブを取得
 */
export declare function getJobQueueStub(env: DidoQEnv, shardId: number): DurableObjectStub;
/**
 * ジョブをD1に挿入
 */
export declare function insertJob<TPayload = unknown>(env: DidoQEnv, config: DidoQConfig, request: JobEnqueueRequest<TPayload>): Promise<void>;
/**
 * ジョブをエンキュー
 */
export declare function enqueueJob<TPayload = unknown>(env: DidoQEnv, config: DidoQConfig, request: JobEnqueueRequest<TPayload>): Promise<{
    jobId: string;
    shardId: number;
}>;
/**
 * ジョブを完了
 */
export declare function completeJob(env: DidoQEnv, config: DidoQConfig, request: JobCompleteRequest): Promise<void>;
/**
 * Heartbeatを送信
 */
export declare function sendHeartbeat(env: DidoQEnv, config: DidoQConfig, jobId: string): Promise<void>;
/**
 * 全シャードの情報を取得
 */
export declare function getAllQueueInfo(env: DidoQEnv, shardCount: number): Promise<AggregatedQueueInfo>;
/**
 * 全シャードをキック
 */
export declare function kickAllShards(env: DidoQEnv, shardCount: number): Promise<{
    success: number;
    failed: number;
    results: Array<{
        shard: number;
        success: boolean;
        error?: string;
    }>;
}>;
/**
 * Durable Object実装
 * TODO: 実際のDO実装が必要な場合はdidoq v1を参照
 */
export declare class DidoQDurableObject {
    private _state;
    private _env;
    constructor(_state: DurableObjectState, _env: DidoQEnv);
    fetch(request: Request): Promise<Response>;
}
