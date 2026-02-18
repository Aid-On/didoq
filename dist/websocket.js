/**
 * DidoQ v2 - WebSocket Support
 *
 * Durable ObjectでWebSocketを管理し、
 * ジョブの状態変更をリアルタイムで通知
 */
/**
 * WebSocket接続を管理するDurable Object
 */
export class JobMonitorDO {
    state;
    env;
    sessions = new Map();
    jobSubscriptions = new Map(); // jobId -> Set<sessionId>
    constructor(state, env) {
        this.state = state;
        this.env = env;
        // アラームを設定してジョブ状態を監視
        this.state.blockConcurrencyWhile(async () => {
            const alarm = await this.state.storage.getAlarm();
            if (!alarm) {
                await this.state.storage.setAlarm(Date.now() + 1000); // 1秒後に初回チェック
            }
        });
    }
    async fetch(request) {
        const url = new URL(request.url);
        switch (url.pathname) {
            case '/websocket':
                return this.handleWebSocketUpgrade(request);
            case '/notify':
                return this.handleNotification(request);
            default:
                return new Response('Not Found', { status: 404 });
        }
    }
    /**
     * WebSocketアップグレード処理
     */
    async handleWebSocketUpgrade(request) {
        const upgradeHeader = request.headers.get('Upgrade');
        if (upgradeHeader !== 'websocket') {
            return new Response('Expected Upgrade: websocket', { status: 426 });
        }
        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);
        // セッションIDを生成
        const sessionId = crypto.randomUUID();
        this.sessions.set(sessionId, server);
        // WebSocketイベントハンドラー
        server.addEventListener('message', async (event) => {
            try {
                const message = JSON.parse(event.data);
                await this.handleWebSocketMessage(sessionId, message);
            }
            catch (error) {
                server.send(JSON.stringify({
                    type: 'error',
                    error: 'Invalid message format'
                }));
            }
        });
        server.addEventListener('close', () => {
            this.handleWebSocketClose(sessionId);
        });
        server.addEventListener('error', () => {
            this.handleWebSocketClose(sessionId);
        });
        // 接続を受け入れ
        server.accept();
        // 接続確認メッセージを送信
        server.send(JSON.stringify({
            type: 'connected',
            sessionId,
            timestamp: Date.now()
        }));
        return new Response(null, {
            status: 101,
            webSocket: client,
        });
    }
    /**
     * WebSocketメッセージ処理
     */
    async handleWebSocketMessage(sessionId, message) {
        const ws = this.sessions.get(sessionId);
        if (!ws)
            return;
        const msg = message;
        switch (msg.type) {
            case 'subscribe':
                await this.subscribeToJob(sessionId, msg.jobId);
                break;
            case 'unsubscribe':
                await this.unsubscribeFromJob(sessionId, msg.jobId);
                break;
            case 'ping':
                ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
                break;
        }
    }
    /**
     * WebSocket切断処理
     */
    handleWebSocketClose(sessionId) {
        // すべてのサブスクリプションを解除
        for (const [jobId, sessions] of this.jobSubscriptions.entries()) {
            sessions.delete(sessionId);
            if (sessions.size === 0) {
                this.jobSubscriptions.delete(jobId);
            }
        }
        this.sessions.delete(sessionId);
    }
    /**
     * ジョブの監視を開始
     */
    async subscribeToJob(sessionId, jobId) {
        const ws = this.sessions.get(sessionId);
        if (!ws)
            return;
        // サブスクリプション管理
        if (!this.jobSubscriptions.has(jobId)) {
            this.jobSubscriptions.set(jobId, new Set());
        }
        this.jobSubscriptions.get(jobId).add(sessionId);
        // 現在のジョブ状態を送信
        const job = await this.getJobStatus(jobId);
        if (job) {
            ws.send(JSON.stringify({
                type: 'job_status',
                jobId,
                status: job.status,
                data: job,
                timestamp: Date.now()
            }));
        }
    }
    /**
     * ジョブの監視を停止
     */
    async unsubscribeFromJob(sessionId, jobId) {
        const sessions = this.jobSubscriptions.get(jobId);
        if (sessions) {
            sessions.delete(sessionId);
            if (sessions.size === 0) {
                this.jobSubscriptions.delete(jobId);
            }
        }
    }
    /**
     * 外部からの通知処理（ジョブ状態が変更された時）
     */
    async handleNotification(request) {
        try {
            const notification = await request.json();
            const { jobId, status, data } = notification;
            // 該当ジョブを監視している全セッションに通知
            const sessions = this.jobSubscriptions.get(jobId);
            if (sessions) {
                const message = JSON.stringify({
                    type: 'job_update',
                    jobId,
                    status,
                    data,
                    timestamp: Date.now()
                });
                for (const sessionId of sessions) {
                    const ws = this.sessions.get(sessionId);
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(message);
                    }
                }
            }
            return new Response('OK');
        }
        catch (error) {
            return new Response('Bad Request', { status: 400 });
        }
    }
    /**
     * アラーム処理（定期的にジョブ状態をチェック）
     */
    async alarm() {
        // 監視中のジョブの状態をチェック
        for (const jobId of this.jobSubscriptions.keys()) {
            const job = await this.getJobStatus(jobId);
            if (job) {
                const sessions = this.jobSubscriptions.get(jobId);
                if (sessions) {
                    const message = JSON.stringify({
                        type: 'heartbeat',
                        jobId,
                        status: job.status,
                        timestamp: Date.now()
                    });
                    for (const sessionId of sessions) {
                        const ws = this.sessions.get(sessionId);
                        if (ws && ws.readyState === WebSocket.OPEN) {
                            ws.send(message);
                        }
                    }
                }
            }
        }
        // 次のアラームを設定（5秒後）
        await this.state.storage.setAlarm(Date.now() + 5000);
    }
    /**
     * ジョブステータスを取得
     */
    async getJobStatus(jobId) {
        const result = await this.env.DB.prepare('SELECT * FROM jobs WHERE id = ?')
            .bind(jobId)
            .first();
        return result;
    }
}
/**
 * WebSocket接続用のヘルパー関数
 */
export function getJobMonitorStub(env, jobId) {
    const namespace = env.JOB_MONITOR || env.JOB_QUEUE;
    if (!namespace)
        throw new Error('No monitor namespace');
    const id = namespace.idFromName(`monitor-${jobId}`);
    return namespace.get(id);
}
/**
 * ジョブ状態変更を通知
 */
export async function notifyJobUpdate(env, jobId, status, data) {
    const stub = getJobMonitorStub(env, jobId);
    await stub.fetch('http://internal/notify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jobId, status, data })
    });
}
