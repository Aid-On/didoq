/**
 * DidoQ v2 Fluent Query Builder
 * Extracted from fluent.ts - list, count, info, kickAll, stream, cleanup
 */

import type { Stream } from '@aid-on/nagare';
import { stream } from '@aid-on/nagare';
import type {
  DidoQConfig,
  DidoQEnv,
  JobStatus,
  D1JobRecord,
  QueueInfo,
} from './types';
import {
  DEFAULT_CONFIG,
  getAllQueueInfo as getAllQueueInfoCore,
  kickAllShards as kickAllShardsCore,
} from './core';
import type { JobEvent } from './fluent';

/**
 * Fluent Queue Manager
 * Queue-wide management and monitoring
 */
export class FluentQueue<TEnv extends DidoQEnv = DidoQEnv> {
  private _env: TEnv;
  private _config: DidoQConfig;
  private _filters: {
    status?: JobStatus[];
    userId?: string;
    jobType?: string;
    shardId?: number;
  } = {};

  constructor(env: TEnv, config?: Partial<DidoQConfig>) {
    this._env = env;
    this._config = { ...DEFAULT_CONFIG, ...config };
  }

  /** Filter by status */
  withStatus(...statuses: JobStatus[]): this {
    this._filters.status = statuses;
    return this;
  }

  /** Filter by user ID */
  forUser(userId: string): this {
    this._filters.userId = userId;
    return this;
  }

  /** Filter by job type */
  ofType(jobType: string): this {
    this._filters.jobType = jobType;
    return this;
  }

  /** Filter by shard ID */
  inShard(shardId: number): this {
    this._filters.shardId = shardId;
    return this;
  }

  /** List jobs matching current filters */
  async list(limit = 100): Promise<D1JobRecord[]> {
    let query = `SELECT * FROM ${this._config.tableName} WHERE 1=1`;
    const bindings: (string | number)[] = [];

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
      .all<D1JobRecord>();

    return result.results || [];
  }

  /** Count jobs matching current filters */
  async count(): Promise<number> {
    let query = `SELECT COUNT(*) as count FROM ${this._config.tableName} WHERE 1=1`;
    const bindings: (string | number)[] = [];

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
      .first<{ count: number }>();

    return result?.count || 0;
  }

  /** Get queue info from all shards */
  async info(): Promise<QueueInfo[]> {
    const aggregated = await getAllQueueInfoCore(this._env, this._config.shardCount || 10);
    return aggregated.shards;
  }

  /** Kick all shards to start processing */
  async kickAll(): Promise<void> {
    await kickAllShardsCore(this._env, this._config.shardCount || 10);
  }

  /** Monitor jobs via nagare Stream */
  stream(): Stream<JobEvent> {
    return stream.create<JobEvent>((controller) => {
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
        } catch (error) {
          controller.error(error as Error);
          clearInterval(pollInterval);
        }
      }, 5000);

      return () => {
        clearInterval(pollInterval);
      };
    });
  }

  /** Delete old completed/failed jobs */
  async cleanup(olderThanMs = 7 * 24 * 60 * 60 * 1000): Promise<number> {
    const cutoff = Date.now() - olderThanMs;

    const result = await this._env.DB.prepare(
      `DELETE FROM ${this._config.tableName}
       WHERE status IN ('completed', 'failed')
       AND completed_at < ?`
    )
      .bind(cutoff)
      .run();

    return result.meta.changes || 0;
  }
}
