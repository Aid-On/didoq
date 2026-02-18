/**
 * Enhanced DidoQ2 Core - Durable Object class definition
 * Job processing logic is in core-enhanced-processing.ts
 * Job handlers are in core-enhanced-handlers.ts
 */
import { DurableObject } from 'cloudflare:workers';
import type { DidoQConfig, DidoQEnv } from './types';
export declare const DEFAULT_CONFIG: DidoQConfig;
/**
 * Enhanced DidoQ Durable Object with Job Processing Loop
 */
export declare class EnhancedDidoQDurableObject extends DurableObject {
    private config;
    private shardId;
    private processingJobs;
    private isProcessing;
    protected env: DidoQEnv;
    constructor(state: DurableObjectState, env: DidoQEnv);
    /**
     * Extract shard ID from Durable Object name.
     * Names are in the format "shard-0", "shard-1", etc.
     */
    private extractShardIdFromName;
    fetch(request: Request): Promise<Response>;
    /** Alarm handler: periodic job processing */
    alarm(): Promise<void>;
    /** Handle enqueue request */
    private handleEnqueue;
    /** Start job processing by setting an alarm */
    private startProcessing;
    /** Run job batch processing with isProcessing guard */
    private runProcessJobBatch;
    /** Get shard statistics */
    private getShardStats;
}
