/**
 * Enhanced Core単体テスト
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { EnhancedDidoQDurableObject } from '../src/core-enhanced';
import type { DurableObjectState } from '@cloudflare/workers-types';

// モック環境の作成
const createMockEnv = () => {
  // Single statement object where bind() returns self
  // This allows both prepare().all and prepare().bind().all to work
  const mockAll = vi.fn().mockResolvedValue({ results: [] });
  const mockRun = vi.fn().mockResolvedValue({ success: true });
  const mockFirst = vi.fn().mockResolvedValue(null);
  const mockStatement: Record<string, unknown> = {
    all: mockAll,
    run: mockRun,
    first: mockFirst,
  };
  mockStatement.bind = vi.fn().mockReturnValue(mockStatement);

  const monitorStub = {
    fetch: vi.fn().mockResolvedValue(new Response('ok')),
  };

  return {
    DB: {
      prepare: vi.fn().mockReturnValue(mockStatement),
    },
    JOB_MONITOR: {
      get: vi.fn(() => monitorStub),
      idFromName: vi.fn((name: string) => ({ name })),
    },
    WORKER_SERVICE: null as unknown, // Service Bindingはオプション
    // Exposed mock references for direct test access
    _mocks: {
      all: mockAll,
      run: mockRun,
      first: mockFirst,
      monitorStub,
    },
  };
};

const createMockState = () => ({
  id: {
    toString: () => 'test-do-id',
    name: 'shard-3', // シャードID = 3
  },
  storage: {
    get: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    list: vi.fn(),
    getAlarm: vi.fn().mockResolvedValue(null),
    setAlarm: vi.fn().mockResolvedValue(void 0),
    deleteAlarm: vi.fn(),
  },
  blockConcurrencyWhile: vi.fn((fn) => fn()),
  waitUntil: vi.fn(),
} as unknown as DurableObjectState);

describe('EnhancedDidoQDurableObject', () => {
  let dobj: EnhancedDidoQDurableObject;
  let mockEnv: ReturnType<typeof createMockEnv>;
  let mockState: ReturnType<typeof createMockState>;

  beforeEach(() => {
    mockEnv = createMockEnv();
    mockState = createMockState();
    dobj = new EnhancedDidoQDurableObject(mockState, mockEnv);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('Constructor', () => {
    it('シャードIDを正しく抽出する', () => {
      const response = dobj.fetch(new Request('http://test/info'));
      response.then(async (res) => {
        const data = await res.json() as Record<string, unknown>;
        expect(data.shardId).toBe(3);
      });
    });
  });

  describe('fetch()', () => {
    it('/kick エンドポイントでアラームを設定する', async () => {
      const response = await dobj.fetch(new Request('http://test/kick'));
      const data = await response.json() as Record<string, unknown>;

      expect(data.success).toBe(true);
      expect(data.message).toBe('Processing started');
      expect(data.shardId).toBe(3);
      expect(mockState.storage.setAlarm).toHaveBeenCalled();
    });

    it('/info エンドポイントで状態を返す', async () => {
      const response = await dobj.fetch(new Request('http://test/info'));
      const data = await response.json() as Record<string, unknown>;

      expect(data).toMatchObject({
        shardId: 3,
        isProcessing: false,
        processingCount: 0,
        config: {
          tableName: 'jobs',
          shardCount: 10,
          websocketEnabled: true,
          flushInterval: 5000,
          batchSize: 100,
          maxRetries: 3,
        },
      });
    });

    it('/stats エンドポイントでシャード統計を返す', async () => {
      mockEnv._mocks.all.mockResolvedValue({
        results: [
          { status: 'pending', count: 5 },
          { status: 'completed', count: 10 },
        ],
      });

      const response = await dobj.fetch(new Request('http://test/stats'));
      const data = await response.json() as Record<string, unknown>;

      expect(data).toMatchObject({
        shardId: 3,
        stats: [
          { status: 'pending', count: 5 },
          { status: 'completed', count: 10 },
        ],
        isProcessing: false,
        processingCount: 0,
      });
    });
  });

  describe('alarm()', () => {
    it('アラーム時にジョブ処理を開始する', async () => {
      const mockJobs = [
        { id: 'job1', type: 'test', payload: {}, status: 'pending' },
        { id: 'job2', type: 'test', payload: {}, status: 'pending' },
      ];

      mockEnv._mocks.all.mockResolvedValue({
        results: mockJobs,
      });
      mockEnv._mocks.run.mockResolvedValue({});

      await dobj.alarm();

      // fetchPendingJobsが呼ばれる
      expect(mockEnv.DB.prepare).toHaveBeenCalledWith(
        expect.stringContaining('UPDATE jobs')
      );
    });

    it('処理中の場合はスキップする', async () => {
      // 最初のアラームを開始
      const promise1 = dobj.alarm();
      // 2回目のアラーム（スキップされるはず）
      const promise2 = dobj.alarm();

      await Promise.all([promise1, promise2]);

      // DB呼び出しは1回だけ
      expect(mockEnv.DB.prepare).toHaveBeenCalledTimes(1);
    });
  });

  describe('Lock Rescue Mechanism', () => {
    it('ロックされたジョブを救済する', async () => {
      const lockedJob = {
        id: 'locked-job',
        status: 'locked',
        locked_at: '2024-01-01T00:00:00Z', // 古いタイムスタンプ
        type: 'test',
        payload: {},
      };

      mockEnv._mocks.all.mockResolvedValue({
        results: [lockedJob],
      });

      await dobj.alarm();

      // ロック救済クエリが含まれているか確認
      const query = mockEnv.DB.prepare.mock.calls[0][0] as string;
      expect(query).toContain("status = 'locked' AND locked_at < datetime('now', '-5 minutes')");
    });

    it('processing状態のジョブも救済する', async () => {
      const stuckJob = {
        id: 'stuck-job',
        status: 'processing',
        started_at: '2024-01-01T00:00:00Z',
        type: 'test',
        payload: {},
      };

      mockEnv._mocks.all.mockResolvedValue({
        results: [stuckJob],
      });

      await dobj.alarm();

      // processing救済クエリが含まれているか確認
      const query = mockEnv.DB.prepare.mock.calls[0][0] as string;
      expect(query).toContain("status = 'processing' AND started_at < datetime('now', '-10 minutes')");
    });
  });

  describe('Job Processing', () => {
    it('Service Bindingがある場合はWorkerに委譲する', async () => {
      const mockWorkerResponse = new Response(JSON.stringify({ result: 'processed' }));
      mockEnv.WORKER_SERVICE = {
        fetch: vi.fn().mockResolvedValue(mockWorkerResponse),
      };

      const job = {
        id: 'job1',
        type: 'custom',
        payload: { data: 'test' },
        status: 'pending',
      };

      mockEnv._mocks.all.mockResolvedValue({
        results: [job],
      });

      await dobj.alarm();

      expect(mockEnv.WORKER_SERVICE.fetch).toHaveBeenCalledWith(
        expect.objectContaining({
          url: 'http://internal/process-job',
        })
      );
    });

    it('Service Bindingがない場合はフォールバック処理を使う', async () => {
      const job = {
        id: 'job1',
        type: 'text-analysis',
        payload: { text: 'Hello' },
        status: 'pending',
      };

      mockEnv._mocks.all.mockResolvedValue({
        results: [job],
      });

      await dobj.alarm();

      // ジョブ完了の更新が呼ばれる
      const updateCalls = mockEnv.DB.prepare.mock.calls.filter(
        call => (call[0] as string).includes('UPDATE jobs')
      );
      expect(updateCalls.length).toBeGreaterThan(0);
    });
  });

  describe('WebSocket Notification', () => {
    it('ジョブ完了時にMonitor DOに通知する', async () => {
      const job = {
        id: 'job1',
        type: 'test',
        payload: {},
        status: 'pending',
      };

      mockEnv._mocks.all.mockResolvedValue({
        results: [job],
      });

      await dobj.alarm();

      // Monitor DOへの通知を確認
      expect(mockEnv.JOB_MONITOR.get).toHaveBeenCalled();
      const monitorStub = mockEnv.JOB_MONITOR.get();
      expect(monitorStub.fetch).toHaveBeenCalledWith(
        'http://internal/notify',
        expect.objectContaining({
          method: 'POST',
        })
      );
    });

    it('シャーディングを使って正しいMonitor DOに通知する', async () => {
      const job = {
        id: 'job-xyz-123',
        type: 'test',
        payload: {},
        status: 'pending',
      };

      mockEnv._mocks.all.mockResolvedValue({
        results: [job],
      });

      await dobj.alarm();

      // getJobMonitorStubが正しくシャードIDを計算していることを確認
      const idFromNameCall = mockEnv.JOB_MONITOR.idFromName.mock.calls[0][0] as string;
      expect(idFromNameCall).toMatch(/^monitor-shard-\d$/);
    });
  });
});