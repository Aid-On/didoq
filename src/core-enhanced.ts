/**
 * Enhanced DidoQ2 Core - Durable Object class definition
 * Job processing logic is in core-enhanced-processing.ts
 * Job handlers are in core-enhanced-handlers.ts
 */

import { DurableObject } from 'cloudflare:workers';
import type { DidoQConfig, DidoQEnv, D1JobRecord } from './types';
import { processJobBatch } from './core-enhanced-processing';
import { processJob, markJobCompleted, markJobFailed, type JobProcessContext } from './core-enhanced-handlers';

export const DEFAULT_CONFIG: DidoQConfig = {
  tableName: 'jobs',
  shardCount: 10,
  websocketEnabled: true,
  flushInterval: 5000,
  batchSize: 100,
  maxRetries: 3,
};

/**
 * Enhanced DidoQ Durable Object with Job Processing Loop
 */
export class EnhancedDidoQDurableObject extends DurableObject {
  private config: DidoQConfig;
  private shardId: number;
  private processingJobs: Set<string> = new Set();
  private isProcessing: boolean = false;

  protected env: DidoQEnv;

  constructor(state: DurableObjectState, env: DidoQEnv) {
    super(state, env);
    this.env = env;
    this.config = DEFAULT_CONFIG;
    this.shardId = this.extractShardIdFromName();
  }

  /**
   * Extract shard ID from Durable Object name.
   * Names are in the format "shard-0", "shard-1", etc.
   */
  private extractShardIdFromName(): number {
    const name = this.ctx.id.name;
    if (!name) {
      return 0;
    }

    const match = name.match(/shard-(\d+)/);
    if (!match || match.length < 2) {
      return 0;
    }

    const shardId = parseInt(match[1], 10);
    if (isNaN(shardId) || shardId < 0) {
      return 0;
    }

    return shardId;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const method = request.method;

    try {
      switch (url.pathname) {
        case '/enqueue':
          if (method === 'POST') {
            return await this.handleEnqueue(request);
          }
          break;

        case '/kick':
          await this.startProcessing();
          this.ctx.waitUntil(this.runProcessJobBatch());
          return Response.json({
            success: true,
            message: 'Processing started',
            shardId: this.shardId,
          });

        case '/info':
          return Response.json({
            shardId: this.shardId,
            isProcessing: this.isProcessing,
            processingCount: this.processingJobs.size,
            config: this.config,
          });

        case '/process':
          if (method === 'POST') {
            const job = await request.json() as D1JobRecord;
            const jobCtx: JobProcessContext = {
              env: this.env, config: this.config,
              shardId: this.shardId, processingJobs: this.processingJobs,
            };
            const result = await processJob(jobCtx, job);
            return Response.json(result);
          }
          break;

        case '/stats': {
          const stats = await this.getShardStats();
          return Response.json(stats);
        }

        default:
          return Response.json({ error: 'Not found' }, { status: 404 });
      }
    } catch (error) {
      return Response.json({
        error: 'Internal server error',
        details: error instanceof Error ? error.message : 'Unknown error',
      }, { status: 500 });
    }

    return Response.json({ error: 'Method not allowed' }, { status: 405 });
  }

  /** Alarm handler: periodic job processing */
  async alarm(): Promise<void> {
    if (this.isProcessing) {
      return;
    }
    await this.runProcessJobBatch();
  }

  /** Handle enqueue request */
  private async handleEnqueue(request: Request): Promise<Response> {
    const jobRequest = await request.json() as Record<string, unknown>;
    const { jobId, userId, jobType, text, outputLanguage } = jobRequest;

    await this.env.DB.prepare(
      `INSERT OR REPLACE INTO jobs (
        id, user_id, job_type, status, shard_id, queue_position,
        input_text, output_language, created_at, queue_name,
        priority, attempt_count, max_attempts
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?)`
    ).bind(
      jobId,
      userId || 'anonymous',
      jobType || 'text',
      'pending',
      this.shardId,
      0,
      text || '',
      outputLanguage || 'ja',
      'default',
      0,
      0,
      3
    ).run();

    return Response.json({
      success: true,
      jobId,
      message: 'Job enqueued',
    });
  }

  /** Start job processing by setting an alarm */
  private async startProcessing(): Promise<void> {
    const currentAlarm = await this.ctx.storage.getAlarm();
    if (currentAlarm) {
      return;
    }
    const alarmTime = Date.now() + 100;
    await this.ctx.storage.setAlarm(alarmTime);
  }

  /** Run job batch processing with isProcessing guard */
  private async runProcessJobBatch(): Promise<void> {
    if (this.isProcessing) return;
    this.isProcessing = true;

    try {
      const jobCtx: JobProcessContext = {
        env: this.env,
        config: this.config,
        shardId: this.shardId,
        processingJobs: this.processingJobs,
      };
      await processJobBatch({
        env: this.env,
        config: this.config,
        shardId: this.shardId,
        ctx: this.ctx,
        processingJobs: this.processingJobs,
        processJobFn: (job) => processJob(jobCtx, job),
        markJobCompletedFn: (jobId, result) => markJobCompleted(this.env, this.config, jobId, result),
        markJobFailedFn: (jobId, error) => markJobFailed(this.env, this.config, jobId, error),
      });
    } catch {
      /* Job batch processing failed; will retry on next alarm */
    } finally {
      this.isProcessing = false;
    }
  }

  /** Get shard statistics */
  private async getShardStats(): Promise<Record<string, unknown>> {
    const stats = await this.env.DB.prepare(`
      SELECT
        status,
        COUNT(*) as count
      FROM ${this.config.tableName}
      WHERE shard_id = ?
      GROUP BY status
    `).bind(this.shardId).all();

    return {
      shardId: this.shardId,
      stats: stats.results,
      isProcessing: this.isProcessing,
      processingCount: this.processingJobs.size,
    };
  }
}
