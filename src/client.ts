/**
 * DidoQ Client - React hooks for job monitoring
 *
 * ブラウザ/クライアントサイド専用。
 * WebSocket接続を優先し、失敗時はHTTPポーリングにフォールバックする。
 */

import { useState, useEffect, useRef, useCallback } from 'react';

export type ConnectionStatus =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'polling'
  | 'reconnecting'
  | 'completed'
  | 'error';

export interface MonitorConfig {
  /** HTTP APIのベースURL (例: https://api.example.com) */
  apiBase: string;
  /** WebSocketのベースURL (例: wss://api.example.com) */
  wsBase: string;
  /** ジョブステータス取得パス (例: /jobs) → {apiBase}{statusPath}/{jobId} */
  statusPath: string;
  /** WebSocket接続パス (例: /ws/jobs) → {wsBase}{wsPath}/{jobId} */
  wsPath: string;
  /** WebSocket再接続の最大試行回数（超えたらHTTPポーリングへ切替） */
  maxRetries: number;
  /** タイムアウト (ms)。0の場合は無効 */
  timeout: number;
}

export interface MonitorCallbacks {
  onComplete: (resultId: string) => void;
  onError: (error: string) => void;
}

export interface JobMonitorResult {
  status: ConnectionStatus;
  isMonitoring: boolean;
  stop: () => void;
  retryCount: number;
}

interface JobStatusResponse {
  status: string;
  result_id?: string;
  resultId?: string;
  error?: string;
}

interface WsJobUpdateData {
  result_id?: string;
  resultId?: string;
  error?: string;
}

interface WsMessage {
  type: string;
  jobId?: string;
  status?: string;
  data?: WsJobUpdateData;
  timestamp?: string | number;
}

const POLL_INTERVAL_MS = 2000;
const WS_RECONNECT_BASE_MS = 1000;

/**
 * ジョブ監視 React hook
 *
 * @param jobId 監視対象のジョブID（null の場合は監視しない）
 * @param config 接続設定
 * @param callbacks 完了・エラー時のコールバック
 */
export function useJobMonitor(
  jobId: string | null,
  config: MonitorConfig,
  callbacks: MonitorCallbacks,
): JobMonitorResult {
  const [status, setStatus] = useState<ConnectionStatus>('idle');
  const [retryCount, setRetryCount] = useState(0);

  const stoppedRef = useRef(false);
  const wsRef = useRef<WebSocket | null>(null);
  const pollTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const timeoutTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const retryCountRef = useRef(0);

  // staleクロージャを避けるためrefで最新値を参照
  const callbacksRef = useRef(callbacks);
  callbacksRef.current = callbacks;
  const configRef = useRef(config);
  configRef.current = config;

  const clearTimers = useCallback(() => {
    if (pollTimerRef.current !== null) {
      clearTimeout(pollTimerRef.current);
      pollTimerRef.current = null;
    }
    if (timeoutTimerRef.current !== null) {
      clearTimeout(timeoutTimerRef.current);
      timeoutTimerRef.current = null;
    }
  }, []);

  const closeWs = useCallback(() => {
    if (wsRef.current) {
      wsRef.current.onopen = null;
      wsRef.current.onclose = null;
      wsRef.current.onerror = null;
      wsRef.current.onmessage = null;
      try { wsRef.current.close(); } catch { /* ignore */ }
      wsRef.current = null;
    }
  }, []);

  const handleComplete = useCallback((resultId: string) => {
    stoppedRef.current = true;
    clearTimers();
    closeWs();
    setStatus('completed');
    callbacksRef.current.onComplete(resultId);
  }, [clearTimers, closeWs]);

  const handleError = useCallback((error: string) => {
    stoppedRef.current = true;
    clearTimers();
    closeWs();
    setStatus('error');
    callbacksRef.current.onError(error);
  }, [clearTimers, closeWs]);

  // HTTPポーリング
  const startPolling = useCallback((jid: string) => {
    if (stoppedRef.current) return;
    setStatus('polling');

    const poll = async () => {
      if (stoppedRef.current) return;
      try {
        const { apiBase, statusPath } = configRef.current;
        const res = await fetch(`${apiBase}${statusPath}/${jid}`);
        if (!res.ok) {
          if (!stoppedRef.current) {
            pollTimerRef.current = setTimeout(poll, POLL_INTERVAL_MS);
          }
          return;
        }
        const data = await res.json() as JobStatusResponse;
        if (stoppedRef.current) return;

        if (data.status === 'completed') {
          handleComplete(data.result_id ?? data.resultId ?? jid);
        } else if (data.status === 'failed' || data.status === 'errored') {
          handleError(data.error ?? 'ジョブが失敗しました');
        } else {
          pollTimerRef.current = setTimeout(poll, POLL_INTERVAL_MS);
        }
      } catch {
        if (!stoppedRef.current) {
          pollTimerRef.current = setTimeout(poll, POLL_INTERVAL_MS);
        }
      }
    };

    poll();
  }, [handleComplete, handleError]);

  // WebSocket接続（再帰的に呼ばれるため前方参照をrefで解決）
  const connectWsRef = useRef<(jid: string) => void>(() => { /* 初期化前 */ });

  const connectWs = useCallback((jid: string) => {
    if (stoppedRef.current) return;

    const currentRetry = retryCountRef.current;
    setStatus(currentRetry > 0 ? 'reconnecting' : 'connecting');

    const { wsBase, wsPath, maxRetries } = configRef.current;

    let ws: WebSocket;
    try {
      ws = new WebSocket(`${wsBase}${wsPath}/${jid}`);
    } catch {
      // WebSocket非対応またはURL不正 → ポーリング
      startPolling(jid);
      return;
    }

    wsRef.current = ws;
    const reconnectDelay = WS_RECONNECT_BASE_MS * Math.pow(2, currentRetry);

    ws.onopen = () => {
      if (stoppedRef.current) { ws.close(); return; }
      setStatus('connected');
      retryCountRef.current = 0;
      setRetryCount(0);
      ws.send(JSON.stringify({ type: 'subscribe', jobId: jid }));
    };

    ws.onmessage = (event: MessageEvent) => {
      if (stoppedRef.current) return;
      try {
        const msg = JSON.parse(event.data as string) as WsMessage;
        if (msg.type !== 'job_update') return;
        // jobId が含まれる場合は一致チェック（含まれない場合は通過させる）
        if (msg.jobId !== undefined && msg.jobId !== jid) return;

        if (msg.status === 'completed') {
          const resultId = msg.data?.result_id ?? msg.data?.resultId ?? jid;
          handleComplete(resultId);
        } else if (msg.status === 'failed' || msg.status === 'errored') {
          handleError(msg.data?.error ?? 'ジョブが失敗しました');
        }
      } catch { /* malformed message, ignore */ }
    };

    ws.onerror = () => {
      // onclose が続けて呼ばれるので処理はそちらで行う
    };

    ws.onclose = () => {
      if (stoppedRef.current) return;
      wsRef.current = null;

      const newRetry = retryCountRef.current + 1;
      retryCountRef.current = newRetry;
      setRetryCount(newRetry);

      if (newRetry >= maxRetries) {
        // WebSocket諦め → HTTPポーリングにフォールバック
        startPolling(jid);
      } else {
        pollTimerRef.current = setTimeout(
          () => connectWsRef.current(jid),
          reconnectDelay,
        );
      }
    };
  }, [handleComplete, handleError, startPolling]);

  // refを常に最新に保つ
  connectWsRef.current = connectWs;

  const stop = useCallback(() => {
    stoppedRef.current = true;
    clearTimers();
    closeWs();
    setStatus('idle');
    setRetryCount(0);
    retryCountRef.current = 0;
  }, [clearTimers, closeWs]);

  useEffect(() => {
    if (!jobId) {
      setStatus('idle');
      return;
    }

    stoppedRef.current = false;
    retryCountRef.current = 0;
    setRetryCount(0);

    // タイムアウト設定
    if (config.timeout > 0) {
      timeoutTimerRef.current = setTimeout(() => {
        if (!stoppedRef.current) {
          handleError('タイムアウトしました');
        }
      }, config.timeout);
    }

    connectWs(jobId);

    return () => {
      stoppedRef.current = true;
      clearTimers();
      closeWs();
    };
    // jobIdが変わった時だけ再実行（config/callbacksはrefで追跡）
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId]);

  const isMonitoring =
    status === 'connecting' ||
    status === 'connected' ||
    status === 'polling' ||
    status === 'reconnecting';

  return { status, isMonitoring, stop, retryCount };
}
