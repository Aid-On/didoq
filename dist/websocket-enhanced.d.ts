/**
 * Enhanced WebSocket Support - N+1 problem elimination
 * Push-based notification with sharding support.
 * Notification methods are in websocket-enhanced-notifications.ts.
 */
import { DurableObject } from 'cloudflare:workers';
/**
 * Enhanced Job Monitor Durable Object
 * Push-based notifications, no polling.
 * Shard-aware: monitors multiple jobs per shard.
 */
export declare class EnhancedJobMonitorDO extends DurableObject {
    private sessions;
    private jobSubscriptions;
    private jobStatusCache;
    private shardId;
    private notificationQueue;
    private isProcessingQueue;
    constructor(state: DurableObjectState, env: Record<string, unknown>);
    private extractShardIdFromName;
    fetch(request: Request): Promise<Response>;
    /** Upgrade HTTP to WebSocket connection */
    private handleWebSocketUpgrade;
    /** Handle incoming WebSocket messages */
    private handleWebSocketMessage;
    /** Dispatch parsed WebSocket message to appropriate handler */
    private dispatchMessage;
    /** Handle subscribe message */
    private handleSubscribe;
    /** Handle batch subscribe message */
    private handleBatchSubscribe;
    private subscribeToJob;
    private unsubscribeFromJob;
    private handleWebSocketClose;
    /** Alarm handler: housekeeping only */
    alarm(): Promise<void>;
    /** Find sockets that have timed out or are unresponsive */
    private findDeadSockets;
    /** Try sending a ping, marking socket as dead on failure */
    private trySendPingOrMarkDead;
    /** Close and clean up dead sockets */
    private cleanupDeadSockets;
    /** Remove cached statuses for jobs with no subscribers */
    private pruneOrphanedCache;
    /** Get monitor statistics */
    private getStats;
}
