/**
 * Enhanced WebSocket Notifications
 * Notification handling extracted from websocket-enhanced.ts
 */
/**
 * Handle a single job update notification.
 */
export async function handleNotification(request, state) {
    try {
        const update = await request.json();
        state.jobStatusCache.set(update.jobId, update);
        state.notificationQueue.push(update);
        if (!state.isProcessingQueue.value) {
            await processNotificationQueue(state);
        }
        return Response.json({ success: true });
    }
    catch {
        /* notification parse failed, return error */
        return Response.json({ error: 'Failed to process notification' }, { status: 500 });
    }
}
/**
 * Handle batch job update notifications.
 */
export async function handleBatchNotification(request, state) {
    try {
        const updates = await request.json();
        for (const update of updates) {
            state.jobStatusCache.set(update.jobId, update);
            state.notificationQueue.push(update);
        }
        if (!state.isProcessingQueue.value) {
            await processNotificationQueue(state);
        }
        return Response.json({
            success: true,
            processed: updates.length,
        });
    }
    catch {
        /* batch parse failed, return error */
        return Response.json({ error: 'Failed to process batch' }, { status: 500 });
    }
}
/**
 * Process the notification queue in batches.
 * Groups updates by jobId and sends to all subscribers.
 */
export async function processNotificationQueue(state) {
    if (state.isProcessingQueue.value)
        return;
    state.isProcessingQueue.value = true;
    try {
        while (state.notificationQueue.length > 0) {
            const batch = state.notificationQueue.splice(0, 50);
            const groupedUpdates = new Map();
            for (const update of batch) {
                groupedUpdates.set(update.jobId, update);
            }
            const promises = [];
            for (const [jobId, update] of groupedUpdates) {
                const subscribers = state.jobSubscriptions.get(jobId);
                if (!subscribers || subscribers.size === 0)
                    continue;
                const message = JSON.stringify({ type: 'job_update', ...update });
                for (const socket of subscribers) {
                    if (socket.readyState === 1) {
                        promises.push(Promise.resolve(socket.send(message)).catch(() => {
                            /* socket send failed, mark as dead */
                        }));
                    }
                }
            }
            if (promises.length > 0) {
                await Promise.allSettled(promises);
            }
            // CPU yielding
            await new Promise(resolve => setTimeout(resolve, 0));
        }
    }
    finally {
        state.isProcessingQueue.value = false;
    }
}
