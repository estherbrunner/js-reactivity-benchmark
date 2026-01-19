import { expect, test } from "vitest";
import { makeGraph, runGraph } from "./benches/reactively/dependencyGraph";
import { frameworkInfo } from "./frameworksList";
import type { FrameworkInfo, TestConfig } from "./util/frameworkTypes";

frameworkInfo.forEach((frameworkInfo) => {
  frameworkTests(frameworkInfo);
});

function makeConfigs(): Record<string, TestConfig> {
  return {
    "static graph": {
      width: 3,
      totalLayers: 3,
      staticFraction: 1,
      nSources: 2,
      readFraction: 1,
      iterations: 2,
      expected: {
        sum: 16,
        count: 11,
      },
    },
    "static graph, read 2/3 of leaves": {
      width: 3,
      totalLayers: 3,
      staticFraction: 1,
      nSources: 2,
      readFraction: 2 / 3,
      iterations: 10,
      expected: {
        sum: 72,
        count: 41,
      },
    },
    "dynamic graph": {
      width: 4,
      totalLayers: 2,
      staticFraction: 0.5,
      nSources: 2,
      readFraction: 1,
      iterations: 10,
      expected: {
        sum: 72,
        count: 22,
      },
    },
    "600000 iterations": {
      width: 10, // can't change for decorator tests
      staticFraction: 1, // can't change for decorator tests
      nSources: 2, // can't change for decorator tests
      totalLayers: 5,
      readFraction: 0.2,
      iterations: 600000,
      expected: {
        sum: 19199968,
        count: 3480019,
      },
    },
    "dynamic graph, read 3/4 of leaves": {
      width: 10,
      totalLayers: 10,
      staticFraction: 3 / 4,
      nSources: 6,
      readFraction: 0.2,
      iterations: 15000,
      expected: {
        sum: 302310782860,
        count: 1155004,
      },
    },
    "width 1000": {
      width: 1000,
      totalLayers: 12,
      staticFraction: 0.95,
      nSources: 4,
      readFraction: 1,
      iterations: 7000,
      expected: {
        sum: 29355933696000,
        count: 1473791,
      },
    },
    "25 sources": {
      width: 1000,
      totalLayers: 5,
      staticFraction: 1,
      nSources: 25,
      readFraction: 1,
      iterations: 3000,
      expected: {
        sum: 1171484375000,
        count: 735756,
      },
    },
    "500 layers deep": {
      width: 5,
      totalLayers: 500,
      staticFraction: 1,
      nSources: 3,
      readFraction: 1,
      iterations: 500,
      expected: {
        sum: 3.0239642676898464e241,
        count: 1246502,
      },
    },
    "100 x 15, read 1/2 of leaves": {
      width: 100,
      totalLayers: 15,
      staticFraction: 0.5,
      nSources: 6,
      readFraction: 1,
      iterations: 2000,
      expected: {
        sum: 15664996402790400,
        count: 1078673, // 1078849,
      },
    },
  };
}

/** some basic tests to validate the reactive framework
 * wrapper works and can run performance tests.
 */
function frameworkTests({ framework, testPullCounts }: FrameworkInfo) {
  test(`${framework.name} | simple dependency executes`, () => {
    const s = framework.signal(2);
    const c = framework.computed(() => s.read() * 2);

    expect(c.read()).toEqual(4);
  });

  test(`${framework.name} | kairo diamond`, () => {
    const iter = framework.withBuild(() => {
      const head = framework.signal(0);
      const current: any[] = [];
      const width = 5;
      for (let i = 0; i < width; i++) {
        current.push(
          framework.computed(() => {
            return head.read() + 1;
          }),
        );
      }
      const sum = framework.computed(() => {
        return current
          .map((x: any) => x.read())
          .reduce((a: number, b: number) => a + b, 0);
      });
      let callCounter = 0;
      framework.effect(() => {
        sum.read();
        // if (framework.name === "cfxR3") console.log("Effect executed");
        callCounter++;
      });

      return () => {
        framework.withBatch(() => {
          head.write(1);
        });
        expect(sum.read()).toBe(2 * width);
        const atleast = 500;
        callCounter = 0;
        for (let i = 0; i < 500; i++) {
          framework.withBatch(() => {
            head.write(i);
          });
          expect(sum.read()).toBe((i + 1) * width);
        }
        if (testPullCounts) expect(callCounter).toBe(atleast);
      };
    });

    // Run the test function
    iter();

    framework.cleanup();
  });

  test(`${framework.name} | kairo unstable`, () => {
    const iter = framework.withBuild(() => {
      const head = framework.signal(0);
      const double = framework.computed(() => head.read() * 2);
      const inverse = framework.computed(() => -head.read());
      const current = framework.computed(() => {
        let result = 0;
        for (let i = 0; i < 20; i++) {
          result += head.read() % 2 ? double.read() : inverse.read();
        }
        return result;
      });

      let callCounter = 0;
      framework.effect(() => {
        current.read();
        callCounter++;
      });

      return () => {
        framework.withBatch(() => {
          head.write(1);
        });
        expect(current.read()).toBe(40);
        const atleast = 100;
        callCounter = 0;
        for (let i = 0; i < 100; i++) {
          framework.withBatch(() => {
            head.write(i);
          });
        }
        if (testPullCounts) expect(callCounter).toBe(atleast);
      };
    });

    iter();
    framework.cleanup();
  });

  test(`${framework.name} | kairo broadPropagation`, () => {
    const iter = framework.withBuild(() => {
      const head = framework.signal(0);
      let last: any = head;
      let callCounter = 0;

      for (let i = 0; i < 50; i++) {
        const current = framework.computed(() => {
          return head.read() + i;
        });
        const current2 = framework.computed(() => {
          return current.read() + 1;
        });
        framework.effect(() => {
          current2.read();
          callCounter++;
        });
        last = current2;
      }

      return () => {
        framework.withBatch(() => {
          head.write(1);
        });
        const atleast = 50 * 50;
        callCounter = 0;
        for (let i = 0; i < 50; i++) {
          framework.withBatch(() => {
            head.write(i);
          });
          expect(last.read()).toBe(i + 50);
        }
        if (testPullCounts) expect(callCounter).toBe(atleast);
      };
    });

    iter();
    framework.cleanup();
  });

  test(`${framework.name} | molBench`, () => {
    function fib(n: number): number {
      if (n < 2) return 1;
      return fib(n - 1) + fib(n - 2);
    }

    function hard(n: number, _log: string) {
      return n + fib(16);
    }

    const numbers = Array.from({ length: 5 }, (_, i) => i);

    const iter = framework.withBuild(() => {
      let res: number[] = [];
      const A = framework.signal(0);
      const B = framework.signal(0);
      const C = framework.computed(() => (A.read() % 2) + (B.read() % 2));
      const D = framework.computed(() =>
        numbers.map((i) => ({ x: i + (A.read() % 2) - (B.read() % 2) })),
      );
      const E = framework.computed(() =>
        hard(C.read() + A.read() + D.read()[0].x, "E"),
      );
      const F = framework.computed(() => hard(D.read()[2].x || B.read(), "F"));
      const G = framework.computed(
        () => C.read() + (C.read() || E.read() % 2) + D.read()[4].x + F.read(),
      );
      // H:
      framework.effect(() => res.push(hard(G.read(), "H")));
      // I:
      framework.effect(() => res.push(G.read()));
      // J:
      framework.effect(() => res.push(hard(F.read(), "J")));

      let i = 0;
      return () => {
        i++;
        res.length = 0;
        framework.withBatch(() => {
          B.write(1);
          A.write(1 + i * 2);
        });
        framework.withBatch(() => {
          A.write(2 + i * 2);
          B.write(2);
        });
        // Just verify it runs without errors
        expect(res.length).toBeGreaterThan(0);
      };
    });

    // Run the test function multiple times
    for (let i = 0; i < 100; i++) {
      iter();
    }
    framework.cleanup();
  });

  const configs = makeConfigs();
  for (const [testName, config] of Object.entries(configs)) {
    test(`${framework.name} | ${testName}`, () => {
      const { graph, counter } = makeGraph(
        framework,
        config.readFraction,
        config,
      );
      const sum = runGraph(graph, config.iterations, framework);
      expect(sum).toEqual(config.expected.sum);
      if (testPullCounts) {
        expect(counter.count).toEqual(config.expected.count);
      }
    });
  }
}
