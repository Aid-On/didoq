/**
 * Enhanced WebSocket Support - N+1 problem elimination
 * Push-based notification with sharding support.
 * Notification methods are in websocket-enhanced-notifications.ts.
 */

import { DurableObject } from 'cloudflare:workers';
import {
  handleNotification,
  handleBatchNotification,
  type NotificationState,
} from './websocket-enhanced-notifications';

interface WebSocketSession {
  socket: WebSocket;
  userId?: string;
  lastPing?: number;
}

interface JobUpdate {
  jobId: string;
  status: string;
  data?: unknown;
  timestamp: string;
}

/**
 * Enhanced Job Monitor Durable Object
 * Push-based notifications, no polling.
 * Shard-aware: monitors multiple jobs per shard.
 */
export class EnhancedJobMonitorDO extends DurableObject {
  private sessions: Map<WebSocket, WebSocketSession> = new Map();
  private jobSubscriptions: Map<string, Set<WebSocket>> = new Map();
  private jobStatusCache: Map<string, JobUpdate> = new Map();
  private shardId: number;

  private notificationQueue: JobUpdate[] = [];
  private isProcessingQueue: { value: boolean } = { value: false };

  constructor(state: DurableObjectState, env: Record<string, unknown>) {
    super(state, env);
    this.shardId = this.extractShardIdFromName();

    this.ctx.blockConcurrencyWhile(async () => {
      const currentAlarm = await this.ctx.storage.getAlarm();
      if (!currentAlarm) {
        await this.ctx.storage.setAlarm(Date.now() + 60000);
      }
    });
  }

  private extractShardIdFromName(): number {
    const name = this.ctx.id.name;
    const match = name?.match(/monitor-shard-(\d+)/);
    return match ? parseInt(match[1], 10) : 0;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get('Upgrade');

    if (upgradeHeader === 'websocket') {
      return this.handleWebSocketUpgrade(request);
    }

    const notifState: NotificationState = {
      jobStatusCache: this.jobStatusCache,
      notificationQueue: this.notificationQueue,
      isProcessingQueue: this.isProcessingQueue,
      jobSubscriptions: this.jobSubscriptions,
    };

    switch (url.pathname) {
      case '/notify':
        return handleNotification(request, notifState);

      case '/batch-notify':
        return handleBatchNotification(request, notifState);

      case '/stats':
        return this.getStats();

      default:
        return new Response('Not found', { status: 404 });
    }
  }

  /** Upgrade HTTP to WebSocket connection */
  private handleWebSocketUpgrade(request: Request): Response {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.ctx.acceptWebSocket(server);

    const session: WebSocketSession = {
      socket: server,
      userId: request.headers.get('X-User-Id') || undefined,
      lastPing: Date.now(),
    };

    this.sessions.set(server, session);

    server.addEventListener('message', (event) => {
      this.handleWebSocketMessage(server, event);
    });

    server.addEventListener('close', () => {
      this.handleWebSocketClose(server);
    });

    server.addEventListener('error', () => {
      this.handleWebSocketClose(server);
    });

    server.send(JSON.stringify({
      type: 'connected',
      timestamp: new Date().toISOString(),
    }));

    // Cloudflare Workers supports status 101 for WebSocket upgrades,
    // but Node's Response constructor does not (range 200-599 only).
    try {
      return new Response(null, { status: 101, webSocket: client } as ResponseInit);
    } catch {
      // Fallback for non-Cloudflare environments (e.g. Node test runtime)
      const response = new Response(null, { status: 200 });
      Object.defineProperty(response, 'webSocket', { value: client });
      Object.defineProperty(response, 'status', { value: 101 });
      return response;
    }
  }

  /** Handle incoming WebSocket messages */
  private handleWebSocketMessage(socket: WebSocket, event: MessageEvent): void {
    try {
      const message = JSON.parse(event.data as string);
      const session = this.sessions.get(socket);
      if (!session) return;

      this.dispatchMessage(socket, session, message);
    } catch {
      /* malformed message, ignore */
    }
  }

  /** Dispatch parsed WebSocket message to appropriate handler */
  private dispatchMessage(
    socket: WebSocket,
    session: WebSocketSession,
    message: Record<string, unknown>,
  ): void {
    switch (message.type) {
      case 'subscribe':
        this.handleSubscribe(socket, message.jobId as string | undefined);
        break;

      case 'unsubscribe':
        if (message.jobId) {
          this.unsubscribeFromJob(socket, message.jobId as string);
        }
        break;

      case 'ping':
        session.lastPing = Date.now();
        socket.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
        break;

      case 'batch_subscribe':
        this.handleBatchSubscribe(socket, message.jobIds as unknown);
        break;
    }
  }

  /** Handle subscribe message */
  private handleSubscribe(socket: WebSocket, jobId: string | undefined): void {
    if (!jobId) return;
    this.subscribeToJob(socket, jobId);
    const cached = this.jobStatusCache.get(jobId);
    if (cached) {
      socket.send(JSON.stringify({ type: 'job_update', ...cached }));
    }
  }

  /** Handle batch subscribe message */
  private handleBatchSubscribe(socket: WebSocket, jobIds: unknown): void {
    if (!Array.isArray(jobIds)) return;
    for (const jobId of jobIds) {
      this.subscribeToJob(socket, jobId);
    }
  }

  private subscribeToJob(socket: WebSocket, jobId: string): void {
    if (!this.jobSubscriptions.has(jobId)) {
      this.jobSubscriptions.set(jobId, new Set());
    }
    const subscribers = this.jobSubscriptions.get(jobId);
    if (subscribers) {
      subscribers.add(socket);
    }
  }

  private unsubscribeFromJob(socket: WebSocket, jobId: string): void {
    const subscribers = this.jobSubscriptions.get(jobId);
    if (subscribers) {
      subscribers.delete(socket);
      if (subscribers.size === 0) {
        this.jobSubscriptions.delete(jobId);
        this.jobStatusCache.delete(jobId);
      }
    }
  }

  private handleWebSocketClose(socket: WebSocket): void {
    for (const [jobId, subscribers] of this.jobSubscriptions.entries()) {
      subscribers.delete(socket);
      if (subscribers.size === 0) {
        this.jobSubscriptions.delete(jobId);
        this.jobStatusCache.delete(jobId);
      }
    }
    this.sessions.delete(socket);
  }

  /** Alarm handler: housekeeping only */
  async alarm(): Promise<void> {
    const deadSockets = this.findDeadSockets();
    this.cleanupDeadSockets(deadSockets);
    this.pruneOrphanedCache();
    await this.ctx.storage.setAlarm(Date.now() + 60000);
  }

  /** Find sockets that have timed out or are unresponsive */
  private findDeadSockets(): WebSocket[] {
    const now = Date.now();
    const TIMEOUT = 120000;
    const deadSockets: WebSocket[] = [];

    for (const [socket, session] of this.sessions) {
      if (session.lastPing && (now - session.lastPing) > TIMEOUT) {
        deadSockets.push(socket);
      } else if (!session.lastPing) {
        this.trySendPingOrMarkDead(socket, now, deadSockets);
      }
    }

    return deadSockets;
  }

  /** Try sending a ping, marking socket as dead on failure */
  private trySendPingOrMarkDead(socket: WebSocket, now: number, deadSockets: WebSocket[]): void {
    try {
      socket.send(JSON.stringify({ type: 'ping', timestamp: now }));
    } catch {
      deadSockets.push(socket);
    }
  }

  /** Close and clean up dead sockets */
  private cleanupDeadSockets(deadSockets: WebSocket[]): void {
    for (const socket of deadSockets) {
      this.handleWebSocketClose(socket);
      try {
        socket.close();
      } catch {
        /* socket close failed, ignore */
      }
    }
  }

  /** Remove cached statuses for jobs with no subscribers */
  private pruneOrphanedCache(): void {
    for (const [jobId] of this.jobStatusCache) {
      if (!this.jobSubscriptions.has(jobId)) {
        this.jobStatusCache.delete(jobId);
      }
    }
  }

  /** Get monitor statistics */
  private getStats(): Response {
    return Response.json({
      activeSessions: this.sessions.size,
      subscribedJobs: this.jobSubscriptions.size,
      cachedStatuses: this.jobStatusCache.size,
      queueLength: this.notificationQueue.length,
      subscriptions: Array.from(this.jobSubscriptions.entries()).map(([jobId, subs]) => ({
        jobId,
        subscriberCount: subs.size,
      })),
    });
  }
}
