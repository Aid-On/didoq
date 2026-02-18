/**
 * Enhanced WebSocket Notifications
 * Notification handling extracted from websocket-enhanced.ts
 */
interface JobUpdate {
    jobId: string;
    status: string;
    data?: unknown;
    timestamp: string;
}
/** Shared state for notification processing */
export interface NotificationState {
    jobStatusCache: Map<string, JobUpdate>;
    notificationQueue: JobUpdate[];
    isProcessingQueue: {
        value: boolean;
    };
    jobSubscriptions: Map<string, Set<WebSocket>>;
}
/**
 * Handle a single job update notification.
 */
export declare function handleNotification(request: Request, state: NotificationState): Promise<Response>;
/**
 * Handle batch job update notifications.
 */
export declare function handleBatchNotification(request: Request, state: NotificationState): Promise<Response>;
/**
 * Process the notification queue in batches.
 * Groups updates by jobId and sends to all subscribers.
 */
export declare function processNotificationQueue(state: NotificationState): Promise<void>;
export {};
