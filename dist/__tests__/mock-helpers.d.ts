import type { DurableObjectId } from '@cloudflare/workers-types';
import type { D1JobRecord } from '../types';
/** Shared mock D1 database for tests */
export declare class MockD1Database {
    private jobs;
    prepare(sql: string): {
        bind: (...params: unknown[]) => {
            all: <T = D1JobRecord>() => Promise<{
                results: T[];
                success: boolean;
            }>;
            run: () => Promise<{
                success: boolean;
            }>;
        };
    };
    private routeRunStatement;
    addJob(job: Partial<D1JobRecord>): void;
    getJobs(): D1JobRecord[];
    getJobById(id: string): D1JobRecord | undefined;
    getJobsByStatus(status: string): D1JobRecord[];
}
/** Basic mock DurableObjectState for tests */
export declare class MockDurableObjectState {
    private alarms;
    private storageMap;
    id: DurableObjectId;
    waitUntil(promise: Promise<unknown>): Promise<void>;
    storage: DurableObjectStorage;
    blockConcurrencyWhile: (fn: () => Promise<void>) => Promise<void>;
    acceptWebSocket: import("vitest").Mock<import("@vitest/spy").Procedure>;
    getWebSockets: import("vitest").Mock<() => never[]>;
}
/** Advanced mock with concurrency simulation */
export declare class AdvancedMockDurableObjectState {
    private alarms;
    private storageMap;
    private waitUntilPromises;
    id: DurableObjectId;
    waitUntil(promise: Promise<unknown>): Promise<void>;
    getPendingPromises(): Promise<unknown>[];
    clearPendingPromises(): void;
    storage: DurableObjectStorage;
}
