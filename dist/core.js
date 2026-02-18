/**
 * DidoQ v2 - Core functionality
 * Independent implementation without didoq v1 dependency
 */
import { DEFAULT_CONFIG } from './types';
export { DEFAULT_CONFIG };
/**
 * シャードIDを計算
 */
export function getShardId(jobId, shardCount) {
    // Simple hash function for consistent sharding
    let hash = 0;
    for (let i = 0; i < jobId.length; i++) {
        hash = ((hash << 5) - hash) + jobId.charCodeAt(i);
        hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash) % shardCount;
}
/**
 * Durable Object スタブを取得
 */
export function getJobQueueStub(env, shardId) {
    const id = env.JOB_QUEUE.idFromName(`shard-${shardId}`);
    return env.JOB_QUEUE.get(id);
}
/**
 * ジョブをD1に挿入
 */
export async function insertJob(env, config, request) {
    const shardId = getShardId(request.id, config.shardCount || 10);
    await env.DB.prepare(`
    INSERT INTO ${config.tableName} (
      id, user_id, status, job_type, shard_id, 
      retry_count, queue_position, created_at, payload
    ) VALUES (?, ?, 'pending', ?, ?, 0, ?, ?, ?)
  `).bind(request.id, request.userId || null, request.jobType, shardId, Date.now(), // queue_position
    Date.now(), // created_at
    request.payload ? JSON.stringify(request.payload) : null).run();
}
/**
 * ジョブをエンキュー
 */
export async function enqueueJob(env, config, request) {
    const shardId = getShardId(request.id, config.shardCount || 10);
    // D1に挿入
    await insertJob(env, config, request);
    // DOにキック
    const stub = getJobQueueStub(env, shardId);
    await stub.fetch('http://internal/kick');
    return { jobId: request.id, shardId };
}
/**
 * ジョブを完了
 */
export async function completeJob(env, config, request) {
    const now = Date.now();
    await env.DB.prepare(`
    UPDATE ${config.tableName}
    SET status = ?, completed_at = ?, result_id = ?, error_message = ?
    WHERE id = ?
  `).bind(request.status, now, request.resultId || null, request.error || null, request.jobId).run();
}
/**
 * Heartbeatを送信
 */
export async function sendHeartbeat(env, config, jobId) {
    await env.DB.prepare(`
    UPDATE ${config.tableName}
    SET last_heartbeat = ?
    WHERE id = ?
  `).bind(Date.now(), jobId).run();
}
/**
 * 全シャードの情報を取得
 */
export async function getAllQueueInfo(env, shardCount) {
    const promises = [];
    for (let i = 0; i < shardCount; i++) {
        const stub = getJobQueueStub(env, i);
        promises.push(stub.fetch('http://internal/info')
            .then(res => res.json())
            .catch(error => ({
            shardId: i,
            pending: 0,
            processing: 0,
            hasAlarm: false,
            error: error.message
        })));
    }
    const shards = await Promise.all(promises);
    const aggregated = {
        totalPending: 0,
        totalProcessing: 0,
        shards: shards.map(s => ({ ...s, shard: s.shardId }))
    };
    for (const shard of shards) {
        aggregated.totalPending += shard.pending;
        aggregated.totalProcessing += shard.processing;
    }
    return aggregated;
}
/**
 * 全シャードをキック
 */
export async function kickAllShards(env, shardCount) {
    const promises = [];
    for (let i = 0; i < shardCount; i++) {
        const stub = getJobQueueStub(env, i);
        promises.push(stub.fetch('http://internal/kick')
            .then((response) => {
            if (response.ok) {
                return { shard: i, success: true };
            }
            else {
                return { shard: i, success: false, error: `HTTP ${response.status}` };
            }
        })
            .catch((error) => {
            return { shard: i, success: false, error: error.message || 'Unknown error' };
        }));
    }
    const results = await Promise.all(promises);
    const success = results.filter(r => r.success).length;
    const failed = results.filter(r => !r.success).length;
    return { success, failed, results };
}
/**
 * Durable Object実装
 * TODO: 実際のDO実装が必要な場合はdidoq v1を参照
 */
export class DidoQDurableObject {
    state;
    env;
    constructor(state, env) {
        this.state = state;
        this.env = env;
        /* state and env stored via parameter properties */
    }
    async fetch(request) {
        const url = new URL(request.url);
        switch (url.pathname) {
            case '/kick':
                // キック処理
                return new Response('kicked');
            case '/info':
                // 情報取得
                return Response.json({
                    shardId: 0,
                    pending: 0,
                    processing: 0,
                    hasAlarm: false
                });
            default:
                return new Response('Not Found', { status: 404 });
        }
    }
}
