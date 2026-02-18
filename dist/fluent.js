/**
 * DidoQ v2 Fluent API
 *
 * Chainable builder pattern for intuitive job queue operations.
 * Query builder methods are in fluent-query.ts.
 */
import { stream } from '@aid-on/nagare';
import { DEFAULT_CONFIG, enqueueJob as enqueueJobCore, } from './core';
import { FluentQueue } from './fluent-query';
export { FluentQueue };
/**
 * Fluent Job Builder
 * Chainable job creation and configuration
 */
export class FluentJob {
    _jobId;
    _userId;
    _jobType = 'default';
    _payload;
    _config;
    _env;
    _processors = [];
    _eventHandlers = [];
    constructor(env, config) {
        this._env = env;
        this._config = { ...DEFAULT_CONFIG, ...config };
        this._jobId = crypto.randomUUID();
    }
    /** Set job ID */
    id(jobId) {
        this._jobId = jobId;
        return this;
    }
    /** Set user ID */
    userId(userId) {
        this._userId = userId;
        return this;
    }
    /** Set job type */
    type(jobType) {
        this._jobType = jobType;
        return this;
    }
    /** Set payload */
    payload(payload) {
        this._payload = payload;
        return this;
    }
    /** Set shard ID */
    shard(_shardId) {
        return this;
    }
    /** Add a job processor */
    process(processor) {
        this._processors.push(processor);
        return this;
    }
    /** Add an event handler */
    on(handler) {
        this._eventHandlers.push(handler);
        return this;
    }
    /** Set timeout */
    timeout(ms) {
        this._config.timeoutMs = ms;
        return this;
    }
    /** Set max retries */
    maxRetries(count) {
        this._config.maxRetryCount = count;
        return this;
    }
    /** Enqueue the job */
    async enqueue() {
        const result = await enqueueJobCore(this._env, this._config, {
            id: this._jobId,
            userId: this._userId,
            jobType: this._jobType,
            payload: this._payload,
        });
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
        }
        catch {
            /* WebSocket notification failed, continue processing */
        }
        return result;
    }
    /** Process job as a nagare Stream */
    stream() {
        return stream.create((controller) => {
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
                            const done = this._handlePollResult(controller, jobId, job);
                            if (done) {
                                clearInterval(pollInterval);
                            }
                        }
                    }
                    catch (error) {
                        controller.error(error);
                        clearInterval(pollInterval);
                    }
                }, 1000);
            }).catch(error => {
                controller.error(error);
            });
        });
    }
    /**
     * Handle a single poll result, emitting events as appropriate.
     * Returns true if the job has reached a terminal state.
     */
    _handlePollResult(controller, jobId, job) {
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
                data: job.parsedPayload,
            });
            controller.complete();
            return true;
        }
        if (job.status === 'failed') {
            controller.next({
                type: 'failed',
                jobId,
                timestamp: job.completed_at || Date.now(),
                error: job.error_message || 'Unknown error',
            });
            controller.complete();
            return true;
        }
        return false;
    }
    /** Get job status from D1 */
    async _getJobStatus(jobId) {
        const result = await this._env.DB.prepare(`SELECT * FROM ${this._config.tableName} WHERE id = ?`)
            .bind(jobId)
            .first();
        if (!result)
            return null;
        return {
            ...result,
            parsedPayload: result.payload ? JSON.parse(result.payload) : undefined,
        };
    }
    /** Emit event to all handlers */
    _emitEvent(event) {
        this._eventHandlers.forEach(handler => handler(event));
    }
}
/**
 * Fluent API entry point
 */
export class DidoQ {
    _env;
    _config;
    constructor(env, config) {
        this._env = env;
        this._config = { ...DEFAULT_CONFIG, ...config };
    }
    /** Create a new job */
    job() {
        return new FluentJob(this._env, this._config);
    }
    /** Get a queue manager */
    queue() {
        return new FluentQueue(this._env, this._config);
    }
    /** Update configuration */
    configure(config) {
        this._config = { ...this._config, ...config };
        return this;
    }
}
/**
 * Create a DidoQ instance
 */
export function didoq(env, config) {
    return new DidoQ(env, config);
}
