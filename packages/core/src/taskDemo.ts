import {
  abortTask,
  batch,
  createEffect,
  createState,
  createTask,
  disposeTask,
  isPending,
  read,
  setSignal,
} from "./frameworks/cfxR3.js";

// Example 1: Basic async task that fetches data based on a signal
async function example1() {
  console.log("\n=== Example 1: Basic Task ===");

  const userId = createState(1);
  const userData = createTask(
    async (prev, signal) => {
      console.log(`Fetching user ${read(userId)}...`);
      // Simulate API call
      await new Promise((resolve) => setTimeout(resolve, 100));
      if (signal.aborted) {
        console.log("Aborted!");
        return prev;
      }
      return { id: read(userId), name: `User ${read(userId)}` };
    },
    { id: 0, name: "Loading..." },
  );

  // Track when task completes
  createEffect(() => {
    const data = read(userData);
    console.log("User data:", data);
  });

  // Initial fetch
  await new Promise((resolve) => setTimeout(resolve, 150));

  // Change userId - should abort previous fetch and start new one
  setSignal(userId, 2);
  await new Promise((resolve) => setTimeout(resolve, 150));

  console.log("Final data:", read(userData));
}

// Example 2: Error handling
async function example2() {
  console.log("\n=== Example 2: Error Handling ===");

  const shouldFail = createState(false);
  const task = createTask(
    async (_prev, _signal) => {
      const fail = read(shouldFail); // Read signal inside task
      await new Promise((resolve) => setTimeout(resolve, 50));
      if (fail) {
        throw new Error("Task failed!");
      }
      return { status: "success" };
    },
    { status: "initial" },
  );

  // Read initial value (triggers task)
  console.log("Initial:", read(task));
  await new Promise((resolve) => setTimeout(resolve, 100));
  console.log("After initial run:", read(task));

  // Now make it fail (this marks task dirty and triggers recomputation)
  console.log("\nChanging shouldFail to true...");
  setSignal(shouldFail, true);

  // Trigger task by reading (returns old value while pending)
  console.log("Value while pending:", read(task));

  // Wait for promise to reject
  await new Promise((resolve) => setTimeout(resolve, 100));

  // Now reading the task should throw the error
  try {
    read(task);
    console.log("ERROR: Should have thrown!");
  } catch (e) {
    console.log("Caught error:", (e as Error).message);
  }

  // Dispose the task clears the error
  disposeTask(task);
  console.log("After dispose (error cleared):", read(task));
}

// Example 3: Abort handling
async function example3() {
  console.log("\n=== Example 3: Manual Abort ===");

  let executionCount = 0;
  const task = createTask(
    async (prev, signal) => {
      executionCount++;
      console.log(`Execution #${executionCount} started`);
      await new Promise((resolve) => setTimeout(resolve, 200));
      if (signal.aborted) {
        console.log(`Execution #${executionCount} was aborted`);
        return prev;
      }
      console.log(`Execution #${executionCount} completed`);
      return { count: executionCount };
    },
    { count: 0 },
  );

  console.log("Initial:", read(task));
  console.log("Is pending?", isPending(task));

  await new Promise((resolve) => setTimeout(resolve, 50));
  console.log("Is pending after 50ms?", isPending(task));

  // Abort the task
  abortTask(task);
  console.log("Aborted!");

  await new Promise((resolve) => setTimeout(resolve, 200));
  console.log("Final:", read(task));
}

// Example 4: Reactive dependencies with abort
async function example4() {
  console.log("\n=== Example 4: Reactive Abort on Dependency Change ===");

  const query = createState("react");
  let fetchCount = 0;

  const searchResults = createTask<{ query: string; results: string[] }>(
    async (prev, signal) => {
      const currentQuery = read(query);
      fetchCount++;
      const fetchId = fetchCount;
      console.log(`[Fetch ${fetchId}] Searching for "${currentQuery}"...`);

      // Simulate network delay
      await new Promise((resolve) => setTimeout(resolve, 150));

      if (signal.aborted) {
        console.log(`[Fetch ${fetchId}] Aborted for query "${currentQuery}"`);
        return prev;
      }

      console.log(`[Fetch ${fetchId}] Completed for query "${currentQuery}"`);
      return {
        query: currentQuery,
        results: [`${currentQuery} result 1`, `${currentQuery} result 2`],
      };
    },
    { query: "", results: [] },
  );

  // Initial fetch
  console.log("Initial:", read(searchResults));
  await new Promise((resolve) => setTimeout(resolve, 50));

  // Change query before first completes - should abort first fetch
  setSignal(query, "vue");
  await new Promise((resolve) => setTimeout(resolve, 50));

  // Change query again - should abort second fetch
  setSignal(query, "svelte");
  await new Promise((resolve) => setTimeout(resolve, 200));

  console.log("Final:", read(searchResults));
}

// Example 5: Batching with tasks
async function example5() {
  console.log("\n=== Example 5: Batching ===");

  const a = createState(1);
  const b = createState(2);
  let taskRunCount = 0;

  const sum = createTask(async (prev, signal) => {
    taskRunCount++;
    console.log(`Task run #${taskRunCount}: Computing ${read(a)} + ${read(b)}`);
    await new Promise((resolve) => setTimeout(resolve, 50));
    if (signal.aborted) return prev;
    return read(a) + read(b);
  }, 0);

  console.log("Initial:", read(sum));
  await new Promise((resolve) => setTimeout(resolve, 100));

  // Without batch - would trigger task twice
  console.log("\nWithout batch:");
  setSignal(a, 5);
  setSignal(b, 10);
  await new Promise((resolve) => setTimeout(resolve, 100));
  console.log("Result:", read(sum), "Task ran", taskRunCount, "times");

  // Reset
  taskRunCount = 0;

  // With batch - should only trigger task once
  console.log("\nWith batch:");
  batch(() => {
    setSignal(a, 20);
    setSignal(b, 30);
  });
  await new Promise((resolve) => setTimeout(resolve, 100));
  console.log("Result:", read(sum), "Task ran", taskRunCount, "times");
}

// Run all examples
async function main() {
  await example1();
  await example2();
  await example3();
  await example4();
  await example5();
  console.log("\n=== All examples completed ===");
}

main().catch(console.error);
