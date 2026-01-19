# Task API Documentation

## Overview

`createTask()` provides async computed signals that seamlessly integrate promise-based operations into the reactive system. Tasks automatically track dependencies, handle cancellation via AbortController, and implement colorless error propagation.

## Key Features

✅ **Automatic Dependency Tracking**: Tasks track reactive dependencies just like computed signals  
✅ **Abort on Change**: In-flight operations are automatically aborted when dependencies change  
✅ **Error Handling**: Errors are caught and rethrown on read() for clean error propagation  
✅ **Pending State Management**: Returns last committed value while new computation is pending  
✅ **Manual Control**: Abort, check pending status, and dispose tasks programmatically

## API Reference

### `createTask<T>(fn, initialValue, options?)`

Creates an async computed signal.

**Parameters:**
- `fn: (prevValue: T, signal: AbortSignal) => Promise<T>` - Async computation function
  - `prevValue`: The last committed value
  - `signal`: AbortSignal to check if computation was cancelled
- `initialValue: T` - Initial value (required, must be non-nullable)
- `options?: SignalOptions<T>` - Optional configuration
  - `equals?: (a: T, b: T) => boolean` - Custom equality function
  - `guard?: (value: unknown) => value is T` - Runtime type guard

**Returns:** `Task<T>`

### `read<T>(task: Task<T>): T`

Reads the task's current value. Throws any error from the last computation.

### `isPending<T>(task: Task<T>): boolean`

Returns `true` if the task is currently executing async work.

### `abortTask<T>(task: Task<T>): void`

Manually aborts the task's in-flight execution.

### `disposeTask<T>(task: Task<T>): void`

Disposes the task, aborting in-flight work, unlinking from the reactive graph, and clearing errors.

## Usage Examples

### Basic Async Computation

```typescript
const userId = createState(1);

const userData = createTask(
  async (prev, signal) => {
    const id = read(userId);
    const response = await fetch(`/api/users/${id}`, { signal });
    return response.json();
  },
  { id: 0, name: "Loading..." }
);

// Reading triggers computation if dependencies changed
console.log(read(userData)); // { id: 0, name: "Loading..." }
await delay(100);
console.log(read(userData)); // { id: 1, name: "User 1" }
```

### Automatic Abort on Dependency Change

```typescript
const query = createState("react");

const searchResults = createTask(
  async (prev, signal) => {
    const q = read(query);
    const response = await fetch(`/api/search?q=${q}`, { signal });
    
    // Check if aborted during fetch
    if (signal.aborted) return prev;
    
    return response.json();
  },
  { results: [] }
);

// Start search for "react"
read(searchResults);

// Changing query aborts the previous fetch and starts a new one
setSignal(query, "vue");
```

### Error Handling (Colorless Error Propagation)

```typescript
const shouldFail = createState(false);

const task = createTask(
  async (prev, signal) => {
    const fail = read(shouldFail);
    if (fail) throw new Error("Task failed!");
    return { status: "success" };
  },
  { status: "initial" }
);

// Error is stored in the task
setSignal(shouldFail, true);
read(task); // Returns old value while pending
await delay(100);

try {
  read(task); // Throws: Error("Task failed!")
} catch (e) {
  console.error(e);
}
```

### Pending State

```typescript
const task = createTask(
  async (prev, signal) => {
    await delay(1000);
    return { data: "loaded" };
  },
  { data: "initial" }
);

console.log(isPending(task)); // true
console.log(read(task));       // { data: "initial" } (returns old value)

await delay(1100);
console.log(isPending(task)); // false
console.log(read(task));       // { data: "loaded" }
```

### Manual Abort

```typescript
const longRunningTask = createTask(
  async (prev, signal) => {
    for (let i = 0; i < 100; i++) {
      if (signal.aborted) return prev;
      await processChunk(i);
    }
    return { complete: true };
  },
  { complete: false }
);

// Start the task
read(longRunningTask);

// Abort it manually
abortTask(longRunningTask);
```

### Batching

```typescript
const a = createState(1);
const b = createState(2);

const sum = createTask(
  async (prev, signal) => {
    await delay(50);
    return read(a) + read(b);
  },
  0
);

// Without batch: triggers task twice
setSignal(a, 5);
setSignal(b, 10);

// With batch: triggers task once
batch(() => {
  setSignal(a, 5);
  setSignal(b, 10);
});
```

## Task Lifecycle

1. **Created**: Task is created with initial value and IDLE state
2. **Triggered**: Dependencies change or read() is called → marks task DIRTY
3. **Running**: Task enters PENDING state, runs async function with AbortSignal
4. **Resolves**: Promise resolves → commits new value, updates subscribers, returns to IDLE
5. **Rejects**: Promise rejects → stores error, keeps old value, marks ERROR state
6. **Aborted**: Dependencies change during execution → abort signal fires, returns to IDLE

## Implementation Details

### Abort Behavior

When a task's dependencies change while it's pending:
- The AbortController's signal is triggered immediately
- The promise continues but the result is ignored if aborted
- A new computation starts with updated dependencies

### Error State

- Errors are stored in `task.error`
- `read(task)` checks for errors and rethrows them
- Errors persist until task succeeds or is disposed
- `disposeTask()` clears errors to prevent throws after disposal

### Value Commits

- Tasks only commit values when promises resolve successfully
- While pending, `read()` returns the last committed value
- This ensures stable, consistent values for downstream computations

### Comparison with Computed

| Feature | Computed | Task |
|---------|----------|------|
| Synchronous | ✅ | ❌ |
| Asynchronous | ❌ | ✅ |
| Abort Support | ❌ | ✅ |
| Error Handling | Throw immediately | Store and rethrow on read |
| Pending State | N/A | Returns old value |

## Best Practices

1. **Always check AbortSignal**: Check `signal.aborted` before expensive operations
2. **Pass signal to fetch**: Use `fetch(url, { signal })` for automatic cancellation
3. **Handle errors gracefully**: Wrap task reads in try-catch or use error boundaries
4. **Dispose when done**: Call `disposeTask()` to clean up resources
5. **Read dependencies early**: Read all dependencies at the start of the function for consistent tracking

## Performance Considerations

- Tasks have minimal overhead when not pending
- Abort operations are instant (synchronous)
- Promise rejections don't block the reactive system
- Multiple dependency changes while pending → only one abort + one new execution