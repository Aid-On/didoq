/**
 * DidoQ v2 Fluent Query Builder
 * Extracted from fluent.ts - list, count, info, kickAll, stream, cleanup
 */
import { stream } from '@aid-on/nagare';
import { DEFAULT_CONFIG, getAllQueueInfo as getAllQueueInfoCore, kickAllShards as kickAllShardsCore, } from './core';
/**
 * Fluent Queue Manager
 * Queue-wide management and monitoring
 */
export class FluentQueue {
    _env;
    _config;
    _filters = {};
    constructor(env, config) {
        this._env = env;
        this._config = { ...DEFAULT_CONFIG, ...config };
    }
    /** Filter by status */
    withStatus(...statuses) {
        this._filters.status = statuses;
        return this;
    }
    /** Filter by user ID */
    forUser(userId) {
        this._filters.userId = userId;
        return this;
    }
    /** Filter by job type */
    ofType(jobType) {
        this._filters.jobType = jobType;
        return this;
    }
    /** Filter by shard ID */
    inShard(shardId) {
        this._filters.shardId = shardId;
        return this;
    }
    /** List jobs matching current filters */
    async list(limit = 100) {
        let query = `SELECT * FROM ${this._config.tableName} WHERE 1=1`;
        const bindings = [];
        if (this._filters.status?.length) {
            query += ` AND status IN (${this._filters.status.map(() => '?').join(',')})`;
            bindings.push(...this._filters.status);
        }
        if (this._filters.userId) {
            query += ` AND user_id = ?`;
            bindings.push(this._filters.userId);
        }
        if (this._filters.jobType) {
            query += ` AND job_type = ?`;
            bindings.push(this._filters.jobType);
        }
        if (this._filters.shardId !== undefined) {
            query += ` AND shard_id = ?`;
            bindings.push(this._filters.shardId);
        }
        query += ` ORDER BY created_at DESC LIMIT ?`;
        bindings.push(limit);
        const result = await this._env.DB.prepare(query)
            .bind(...bindings)
            .all();
        return result.results || [];
    }
    /** Count jobs matching current filters */
    async count() {
        let query = `SELECT COUNT(*) as count FROM ${this._config.tableName} WHERE 1=1`;
        const bindings = [];
        if (this._filters.status?.length) {
            query += ` AND status IN (${this._filters.status.map(() => '?').join(',')})`;
            bindings.push(...this._filters.status);
        }
        if (this._filters.userId) {
            query += ` AND user_id = ?`;
            bindings.push(this._filters.userId);
        }
        if (this._filters.jobType) {
            query += ` AND job_type = ?`;
            bindings.push(this._filters.jobType);
        }
        if (this._filters.shardId !== undefined) {
            query += ` AND shard_id = ?`;
            bindings.push(this._filters.shardId);
        }
        const result = await this._env.DB.prepare(query)
            .bind(...bindings)
            .first();
        return result?.count || 0;
    }
    /** Get queue info from all shards */
    async info() {
        const aggregated = await getAllQueueInfoCore(this._env, this._config.shardCount || 10);
        return aggregated.shards;
    }
    /** Kick all shards to start processing */
    async kickAll() {
        await kickAllShardsCore(this._env, this._config.shardCount || 10);
    }
    /** Monitor jobs via nagare Stream */
    stream() {
        return stream.create((controller) => {
            const pollInterval = setInterval(async () => {
                try {
                    const jobs = await this.list();
                    jobs.forEach(job => {
                        if (job.status === 'processing' && job.last_heartbeat) {
                            controller.next({
                                type: 'processing',
                                jobId: job.id,
                                timestamp: job.last_heartbeat,
                            });
                        }
                    });
                }
                catch (error) {
                    controller.error(error);
                    clearInterval(pollInterval);
                }
            }, 5000);
            return () => {
                clearInterval(pollInterval);
            };
        });
    }
    /** Delete old completed/failed jobs */
    async cleanup(olderThanMs = 7 * 24 * 60 * 60 * 1000) {
        const cutoff = Date.now() - olderThanMs;
        const result = await this._env.DB.prepare(`DELETE FROM ${this._config.tableName}
       WHERE status IN ('completed', 'failed')
       AND completed_at < ?`)
            .bind(cutoff)
            .run();
        return result.meta.changes || 0;
    }
}
