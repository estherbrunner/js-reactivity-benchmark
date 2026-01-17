import { nextTick } from "../util/asyncUtil";
import { benchmarkWithMemory, fastestTest } from "../util/benchRepeat";
import type { FrameworkInfo } from "../util/frameworkTypes";
import type { PerfResultCallback } from "../util/perfLogging";
import { avoidablePropagation } from "./kairo/avoidable";
import { broadPropagation } from "./kairo/broad";
import { deepPropagation } from "./kairo/deep";
import { diamond } from "./kairo/diamond";
import { mol } from "./kairo/molBench";
import { mux } from "./kairo/mux";
import { repeatedObservers } from "./kairo/repeated";
import { triangle } from "./kairo/triangle";
import { unstable } from "./kairo/unstable";

const cases = [
  { name: "avoidablePropagation", fn: avoidablePropagation },
  { name: "broadPropagation", fn: broadPropagation },
  { name: "deepPropagation", fn: deepPropagation },
  { name: "diamond", fn: diamond },
  { name: "mux", fn: mux },
  { name: "repeatedObservers", fn: repeatedObservers },
  { name: "triangle", fn: triangle },
  { name: "unstable", fn: unstable },
  { name: "molBench", fn: mol },
];

export async function kairoBench(
  frameworkInfo: FrameworkInfo[],
  logPerfResult: PerfResultCallback,
) {
  // warmup
  for (const c of cases) {
    for (const { framework } of frameworkInfo) {
      const iter = framework.withBuild(() => c.fn(framework));

      iter();
      iter();

      await nextTick();
      iter();

      framework.cleanup();
    }
  }

  if (globalThis.gc) (globalThis.gc!(), globalThis.gc!());
  await nextTick();

  // actual benchmark
  for (const c of cases) {
    for (const { framework } of frameworkInfo) {
      const iter = framework.withBuild(() => {
        const iter = c.fn(framework);
        return iter;
      });

      iter();
      iter();
      await nextTick();

      iter();
      await nextTick();

      if (globalThis.gc) {
        // Use benchmarkWithMemory when GC is available
        const result = await benchmarkWithMemory(10, () => {
          for (let i = 0; i < 500; i++) {
            iter();
          }
          return 0; // dummy return value
        });

        framework.cleanup();

        logPerfResult({
          framework: framework.name,
          test: c.name,
          time: result.time,
          memoryUsed: result.memory?.memoryUsed,
          heapUsed: result.memory?.heapUsed,
          gcTime: result.memory?.gcTime,
        });
      } else {
        // Fallback to simple timing
        const { time } = await fastestTest(10, () => {
          for (let i = 0; i < 500; i++) {
            iter();
          }
        });

        framework.cleanup();

        logPerfResult({
          framework: framework.name,
          test: c.name,
          time,
        });
      }
    }
  }
}
