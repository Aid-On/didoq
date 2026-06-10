/**
 * DidoQ Client - React hooks for job monitoring
 *
 * ブラウザ/クライアントサイド専用。
 * WebSocket接続を優先し、失敗時はHTTPポーリングにフォールバックする。
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import { nextPollDelay, wsReconnectDelay } from './poll-schedule';

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
  onProgress?: (phase: string, data: unknown) => void;
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
  phase?: string;
  resultId?: string;
  error?: string;
  timestamp?: string | number;
}

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
  const wsReconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const timeoutTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const retryCountRef = useRef(0);
  /** ポーリングループが稼働中か（多重起動の防止と WS 復帰時の停止に使う） */
  const pollingActiveRef = useRef(false);
  /** 適応間隔の段数（nextPollDelay へ渡す。ループ起動ごとにリセット） */
  const pollCountRef = useRef(0);

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
    if (wsReconnectTimerRef.current !== null) {
      clearTimeout(wsReconnectTimerRef.current);
      wsReconnectTimerRef.current = null;
    }
    if (timeoutTimerRef.current !== null) {
      clearTimeout(timeoutTimerRef.current);
      timeoutTimerRef.current = null;
    }
    pollingActiveRef.current = false;
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

  /**
   * 単発のステータス照会。終端（complete/error をディスパッチ済み）なら true。
   * 一時エラー（ネットワーク・非2xx）は false（呼び出し側が次回に再試行）。
   */
  const checkStatus = useCallback(async (jid: string): Promise<boolean> => {
    try {
      const { apiBase, statusPath } = configRef.current;
      const res = await fetch(`${apiBase}${statusPath}/${jid}`);
      if (stoppedRef.current) return true;
      if (!res.ok) return false;
      const data = await res.json() as JobStatusResponse;
      if (stoppedRef.current) return true;

      if (data.status === 'completed') {
        handleComplete(data.result_id ?? data.resultId ?? jid);
        return true;
      }
      if (data.status === 'failed' || data.status === 'errored') {
        handleError(data.error ?? 'ジョブが失敗しました');
        return true;
      }
      return false;
    } catch {
      return false;
    }
  }, [handleComplete, handleError]);

  /**
   * HTTPポーリング（適応間隔: 800ms から ×1.5 で疎にし上限 4s、±20% ジッタ）。
   *
   * @param background true のときは WS 再接続と併走する安全網として動く:
   *   status は変えず（WS 側が 'reconnecting' を表示）、WS が復帰したら
   *   stopPolling で止まる。WS が maxRetries を使い切ったら background=false で
   *   呼ばれ、status も 'polling' に切り替わる。
   */
  const startPolling = useCallback((jid: string, background = false) => {
    if (stoppedRef.current) return;
    if (!background) setStatus('polling');
    if (pollingActiveRef.current) return; // ループは多重起動しない（status 更新のみ）
    pollingActiveRef.current = true;
    pollCountRef.current = 0;

    const poll = async () => {
      if (stoppedRef.current || !pollingActiveRef.current) return;
      const terminal = await checkStatus(jid);
      if (terminal || stoppedRef.current || !pollingActiveRef.current) return;
      pollTimerRef.current = setTimeout(poll, nextPollDelay(pollCountRef.current++));
    };

    poll();
  }, [checkStatus]);

  /** ポーリングループだけ止める（WS 復帰時。全体タイムアウトは維持） */
  const stopPolling = useCallback(() => {
    pollingActiveRef.current = false;
    if (pollTimerRef.current !== null) {
      clearTimeout(pollTimerRef.current);
      pollTimerRef.current = null;
    }
  }, []);

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
    const reconnectDelay = wsReconnectDelay(currentRetry, WS_RECONNECT_BASE_MS);

    ws.onopen = () => {
      if (stoppedRef.current) { ws.close(); return; }
      stopPolling(); // WS が権威になったので安全網ポーリングを止める
      setStatus('connected');
      retryCountRef.current = 0;
      setRetryCount(0);
      ws.send(JSON.stringify({ type: 'subscribe', jobId: jid }));
    };

    ws.onmessage = (event: MessageEvent) => {
      if (stoppedRef.current) return;
      try {
        const msg = JSON.parse(event.data as string) as WsMessage;

        // Direct messages from JobQueueDO (complete/error/progress)
        if (msg.type === 'complete') {
          const resultId = msg.resultId ?? msg.data?.result_id ?? msg.data?.resultId ?? jid;
          handleComplete(resultId);
          return;
        }
        if (msg.type === 'error') {
          handleError(msg.error ?? msg.data?.error ?? 'ジョブが失敗しました');
          return;
        }
        if (msg.type === 'progress') {
          callbacksRef.current.onProgress?.(msg.phase ?? '', msg.data);
          return;
        }

        // Legacy: job_update format
        if (msg.type !== 'job_update') return;
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
        // WS 再接続を待つ間も安全網ポーリングを併走させる。
        // 旧実装は再接続が全滅するまでポーリングを始めなかったため、
        // WS が塞がれた環境では完了検知まで累計 2^maxRetries 秒待ちだった。
        startPolling(jid, true);
        wsReconnectTimerRef.current = setTimeout(
          () => connectWsRef.current(jid),
          reconnectDelay,
        );
      }
    };
  }, [handleComplete, handleError, startPolling, stopPolling]);

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
    pollingActiveRef.current = false;
    pollCountRef.current = 0;

    // タイムアウト設定
    if (config.timeout > 0) {
      timeoutTimerRef.current = setTimeout(() => {
        if (!stoppedRef.current) {
          handleError('タイムアウトしました');
        }
      }, config.timeout);
    }

    connectWs(jobId);
    // WS の購読確立前にジョブが完了している競合（短いジョブで起きる）を
    // 1 回だけの即時照会で拾う。未完了なら何もしない（WS/ポーリングに任せる）
    void checkStatus(jobId);

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
