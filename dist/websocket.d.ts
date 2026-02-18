/**
 * DidoQ v2 - WebSocket Support
 *
 * Durable ObjectでWebSocketを管理し、
 * ジョブの状態変更をリアルタイムで通知
 */
import type { DidoQEnv } from './types';
/**
 * WebSocket接続を管理するDurable Object
 */
export declare class JobMonitorDO {
    private _state;
    private _env;
    private sessions;
    private jobSubscriptions;
    constructor(state: DurableObjectState, env: DidoQEnv);
    fetch(request: Request): Promise<Response>;
    /**
     * WebSocketアップグレード処理
     */
    private handleWebSocketUpgrade;
    /**
     * WebSocketメッセージ処理
     */
    private handleWebSocketMessage;
    /**
     * WebSocket切断処理
     */
    private handleWebSocketClose;
    /**
     * ジョブの監視を開始
     */
    private subscribeToJob;
    /**
     * ジョブの監視を停止
     */
    private unsubscribeFromJob;
    /**
     * 外部からの通知処理（ジョブ状態が変更された時）
     */
    private handleNotification;
    /**
     * アラーム処理（定期的にジョブ状態をチェック）
     */
    alarm(): Promise<void>;
    /**
     * 単一ジョブの状態をチェックし、サブスクライバーに通知
     */
    private checkAndNotifyJob;
    /**
     * セッション群にメッセージをブロードキャスト
     */
    private broadcastToSessions;
    /**
     * ジョブステータスを取得
     */
    private getJobStatus;
}
/**
 * WebSocket接続用のヘルパー関数
 */
export declare function getJobMonitorStub(env: DidoQEnv, jobId: string): DurableObjectStub;
/**
 * ジョブ状態変更を通知
 */
export declare function notifyJobUpdate(env: DidoQEnv, jobId: string, status: string, data?: unknown): Promise<void>;
