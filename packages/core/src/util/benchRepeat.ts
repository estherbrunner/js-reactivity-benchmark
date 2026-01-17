import { nextTick } from "./asyncUtil";
import { TimingResult } from "./perfTests";

export interface MemorySnapshot {
  heapUsed: number; // bytes
  heapTotal: number; // bytes
  external: number; // bytes
}

export interface MemoryResult {
  memoryUsed: number; // MB - total memory increase
  heapUsed: number; // MB - heap memory used
  gcTime?: number; // ms - time spent in GC
}

function getMemoryUsage(): MemorySnapshot | null {
  if (typeof process !== "undefined" && process.memoryUsage) {
    const mem = process.memoryUsage();
    return {
      heapUsed: mem.heapUsed,
      heapTotal: mem.heapTotal,
      external: mem.external,
    };
  }
  // Bun also supports process.memoryUsage
  return null;
}

function forceGC(): void {
  if (globalThis.gc) {
    gc!();
  }
}

/** benchmark a function n times, returning the fastest result and associated timing */
export async function fastestTest<T>(
  times: number,
  fn: () => T,
): Promise<TimingResult<T>> {
  const results: TimingResult<T>[] = [];
  for (let i = 0; i < times; i++) {
    await nextTick();
    const run = runTimed(fn);
    results.push(run);
  }
  const fastest = results.reduce((a, b) => (a.time < b.time ? a : b));

  return fastest;
}

/**
 * Benchmark a function with memory tracking.
 *
 * Memory Measurement Approach:
 * - Measures runtime memory overhead (not setup/initialization memory)
 * - Forces GC before measurement to establish a clean baseline
 * - Tracks peak memory usage across all benchmark runs
 * - Returns timing from the fastest run, but memory from peak across all runs
 *
 * Note: Small or zero heap values indicate the function has minimal runtime
 * memory overhead beyond what was allocated during initialization.
 */
export async function benchmarkWithMemory<T>(
  times: number,
  fn: () => T,
): Promise<TimingResult<T> & { memory?: MemoryResult }> {
  // Force GC and wait to settle - establishes clean baseline
  forceGC();
  forceGC();
  await nextTick();
  await nextTick();

  const memBefore = getMemoryUsage();
  let peakHeap = 0;
  let peakTotal = 0;

  // Run tests and track peak memory across all runs
  const results: TimingResult<T>[] = [];
  for (let i = 0; i < times; i++) {
    await nextTick();

    // Measure before and after each run
    const beforeRun = getMemoryUsage();
    const run = runTimed(fn);
    const afterRun = getMemoryUsage();

    // Track peak usage across all runs
    if (afterRun && beforeRun) {
      const heapUsed = afterRun.heapUsed;
      const totalUsed = afterRun.heapTotal + afterRun.external;
      if (heapUsed > peakHeap) peakHeap = heapUsed;
      if (totalUsed > peakTotal) peakTotal = totalUsed;
    }

    results.push(run);
  }

  const fastest = results.reduce((a, b) => (a.time < b.time ? a : b));

  // Measure GC time
  const gcStart = performance.now();
  forceGC();
  const gcTime = performance.now() - gcStart;

  let memory: MemoryResult | undefined;
  if (memBefore && peakHeap > 0 && peakTotal > 0) {
    // Calculate delta from baseline using peak values
    // This avoids negative values that would occur from post-GC measurements
    const heapUsed = peakHeap - memBefore.heapUsed;
    const totalUsed = peakTotal - (memBefore.heapTotal + memBefore.external);

    memory = {
      memoryUsed: Math.max(0, totalUsed / (1024 * 1024)), // Convert to MB, clamp to non-negative
      heapUsed: Math.max(0, heapUsed / (1024 * 1024)), // Convert to MB, clamp to non-negative
      gcTime: gcTime,
    };
  }

  return { ...fastest, memory };
}

export interface TimedResult<T> {
  result: T;
  time: number;
}

/** run a function, recording how long it takes */
export function runTimed<T>(fn: () => T): TimedResult<T> {
  const start = performance.now();
  const result = fn();
  const time = performance.now() - start;
  return { result, time };
}
