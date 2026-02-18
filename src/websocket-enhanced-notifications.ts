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
  isProcessingQueue: { value: boolean };
  jobSubscriptions: Map<string, Set<WebSocket>>;
}

/**
 * Handle a single job update notification.
 */
export async function handleNotification(
  request: Request,
  state: NotificationState,
): Promise<Response> {
  try {
    const update = await request.json() as JobUpdate;

    state.jobStatusCache.set(update.jobId, update);
    state.notificationQueue.push(update);

    if (!state.isProcessingQueue.value) {
      await processNotificationQueue(state);
    }

    return Response.json({ success: true });
  } catch {
    /* notification parse failed, return error */
    return Response.json({ error: 'Failed to process notification' }, { status: 500 });
  }
}

/**
 * Handle batch job update notifications.
 */
export async function handleBatchNotification(
  request: Request,
  state: NotificationState,
): Promise<Response> {
  try {
    const updates = await request.json() as JobUpdate[];

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
  } catch {
    /* batch parse failed, return error */
    return Response.json({ error: 'Failed to process batch' }, { status: 500 });
  }
}

/**
 * Process the notification queue in batches.
 * Groups updates by jobId and sends to all subscribers.
 */
export async function processNotificationQueue(
  state: NotificationState,
): Promise<void> {
  if (state.isProcessingQueue.value) return;
  state.isProcessingQueue.value = true;

  try {
    while (state.notificationQueue.length > 0) {
      const batch = state.notificationQueue.splice(0, 50);
      await processBatch(batch, state);

      // CPU yielding
      await new Promise(resolve => setTimeout(resolve, 0));
    }
  } finally {
    state.isProcessingQueue.value = false;
  }
}

/**
 * Process a single batch of updates by grouping and sending to subscribers.
 */
async function processBatch(
  batch: JobUpdate[],
  state: NotificationState,
): Promise<void> {
  const groupedUpdates = groupByJobId(batch);
  const promises: Promise<void>[] = [];

  for (const [jobId, update] of groupedUpdates) {
    const subscribers = state.jobSubscriptions.get(jobId);
    if (!subscribers || subscribers.size === 0) continue;

    const message = JSON.stringify({ type: 'job_update', ...update });
    sendToSubscribers(subscribers, message, promises);
  }

  if (promises.length > 0) {
    await Promise.allSettled(promises);
  }
}

/** Group updates by jobId, keeping only the latest per job */
function groupByJobId(batch: JobUpdate[]): Map<string, JobUpdate> {
  const grouped = new Map<string, JobUpdate>();
  for (const update of batch) {
    grouped.set(update.jobId, update);
  }
  return grouped;
}

/** Send a message to all open subscribers, collecting promises */
function sendToSubscribers(
  subscribers: Set<WebSocket>,
  message: string,
  promises: Promise<void>[],
): void {
  for (const socket of subscribers) {
    if (socket.readyState === 1) {
      promises.push(
        Promise.resolve(socket.send(message)).catch(() => {
          /* socket send failed */
        })
      );
    }
  }
}
