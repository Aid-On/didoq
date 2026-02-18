/**
 * DidoQ v2 Fluent API
 *
 * Chainable builder pattern for intuitive job queue operations.
 * Query builder methods are in fluent-query.ts.
 */

import type { Stream } from '@aid-on/nagare';
import { stream } from '@aid-on/nagare';
import type {
  DidoQConfig,
  DidoQEnv,
  D1JobRecord,
  JobProcessor,
} from './types';
import {
  DEFAULT_CONFIG,
  enqueueJob as enqueueJobCore,
} from './core';
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
export class FluentJob<TEnv extends DidoQEnv = DidoQEnv, TPayload = unknown> {
  private _jobId: string;
  private _userId?: string;
  private _jobType = 'default';
  private _payload?: TPayload;
  private _config: DidoQConfig;
  private _env: TEnv;
  private _processors: JobProcessor<TPayload>[] = [];
  private _eventHandlers: Array<(event: JobEvent<TPayload>) => void> = [];

  constructor(env: TEnv, config?: Partial<DidoQConfig>) {
    this._env = env;
    this._config = { ...DEFAULT_CONFIG, ...config };
    this._jobId = crypto.randomUUID();
  }

  /** Set job ID */
  id(jobId: string): this {
    this._jobId = jobId;
    return this;
  }

  /** Set user ID */
  userId(userId: string): this {
    this._userId = userId;
    return this;
  }

  /** Set job type */
  type(jobType: string): this {
    this._jobType = jobType;
    return this;
  }

  /** Set payload */
  payload(payload: TPayload): this {
    this._payload = payload;
    return this;
  }

  /** Set shard ID */
  shard(_shardId: number): this {
    return this;
  }

  /** Add a job processor */
  process(processor: JobProcessor<TPayload>): this {
    this._processors.push(processor);
    return this;
  }

  /** Add an event handler */
  on(handler: (event: JobEvent<TPayload>) => void): this {
    this._eventHandlers.push(handler);
    return this;
  }

  /** Set timeout */
  timeout(ms: number): this {
    this._config.timeoutMs = ms;
    return this;
  }

  /** Set max retries */
  maxRetries(count: number): this {
    this._config.maxRetryCount = count;
    return this;
  }

  /** Enqueue the job */
  async enqueue(): Promise<{ jobId: string; shardId: number }> {
    const result = await enqueueJobCore(
      this._env,
      this._config,
      {
        id: this._jobId,
        userId: this._userId,
        jobType: this._jobType,
        payload: this._payload,
      }
    );

    this._emitEvent({
      type: 'enqueued',
      jobId: this._jobId,
      timestamp: Date.now(),
      data: this._payload,
    });

    try {
      const { notifyJobUpdate } = await import('./websocket');
      await notifyJobUpdate(this._env, this._jobId, 'enqueued', {
        payload: this._payload,
      });
    } catch {
      /* WebSocket notification failed, continue processing */
    }

    return result;
  }

  /** Process job as a nagare Stream */
  stream(): Stream<JobEvent<TPayload>> {
    return stream.create<JobEvent<TPayload>>((controller) => {
      this.enqueue().then(({ jobId }) => {
        controller.next({
          type: 'enqueued',
          jobId,
          timestamp: Date.now(),
          data: this._payload,
        });

        const pollInterval = setInterval(async () => {
          try {
            const job = await this._getJobStatus(jobId);

            if (job) {
              if (job.status === 'processing' && job.last_heartbeat) {
                controller.next({
                  type: 'heartbeat',
                  jobId,
                  timestamp: job.last_heartbeat,
                });
              }

              if (job.status === 'completed') {
                controller.next({
                  type: 'completed',
                  jobId,
                  timestamp: job.completed_at || Date.now(),
                  data: job.parsedPayload as TPayload,
                });
                clearInterval(pollInterval);
                controller.complete();
              }

              if (job.status === 'failed') {
                controller.next({
                  type: 'failed',
                  jobId,
                  timestamp: job.completed_at || Date.now(),
                  error: job.error_message || 'Unknown error',
                });
                clearInterval(pollInterval);
                controller.complete();
              }
            }
          } catch (error) {
            controller.error(error as Error);
            clearInterval(pollInterval);
          }
        }, 1000);
      }).catch(error => {
        controller.error(error as Error);
      });
    });
  }

  /** Get job status from D1 */
  private async _getJobStatus(jobId: string): Promise<(D1JobRecord & { parsedPayload?: TPayload }) | null> {
    const result = await this._env.DB.prepare(
      `SELECT * FROM ${this._config.tableName} WHERE id = ?`
    )
      .bind(jobId)
      .first<D1JobRecord>();

    if (!result) return null;

    return {
      ...result,
      parsedPayload: result.payload ? JSON.parse(result.payload) : undefined,
    };
  }

  /** Emit event to all handlers */
  private _emitEvent(event: JobEvent<TPayload>): void {
    this._eventHandlers.forEach(handler => handler(event));
  }
}

/**
 * Fluent API entry point
 */
export class DidoQ<TEnv extends DidoQEnv = DidoQEnv> {
  private _env: TEnv;
  private _config: DidoQConfig;

  constructor(env: TEnv, config?: Partial<DidoQConfig>) {
    this._env = env;
    this._config = { ...DEFAULT_CONFIG, ...config };
  }

  /** Create a new job */
  job<TPayload = unknown>(): FluentJob<TEnv, TPayload> {
    return new FluentJob<TEnv, TPayload>(this._env, this._config);
  }

  /** Get a queue manager */
  queue(): FluentQueue<TEnv> {
    return new FluentQueue(this._env, this._config);
  }

  /** Update configuration */
  configure(config: Partial<DidoQConfig>): this {
    this._config = { ...this._config, ...config };
    return this;
  }
}

/**
 * Create a DidoQ instance
 */
export function didoq<TEnv extends DidoQEnv = DidoQEnv>(
  env: TEnv,
  config?: Partial<DidoQConfig>
): DidoQ<TEnv> {
  return new DidoQ<TEnv>(env, config);
}
