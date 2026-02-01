# Signal Graph Specification (Flags + Linked Edges)

This document describes a dynamic, glitch-free signal graph for JavaScript with a focus on minimal hot-path overhead. It supports memoized computeds, lazy evaluation, conditional dependencies, and deterministic cleanup.
The design is based on plain objects, detached traversal logic, and intrusive linked edges, with performance-driven simplifications aligned to the current implementation.

⸻

## 1. Core Concepts

Nodes

The graph consists of three kinds of nodes:
	•	State
Mutable source of truth. Does not depend on other nodes.
	•	Computed
Memoized, derived value. Recomputes lazily when invalidated.
	•	Effect
Sink node for side effects. Runs when dependencies change; not memoized.

Edges

Dependencies are represented explicitly as Link objects, each connecting:

source (State | Computed) → target (Computed | Effect)

Each link is stored intrusively in:
	•	the source’s subscriber list
	•	the target’s dependency list

⸻

## 2. Node State Flags (“Colors”)

Each Computed and Effect has a bitset of flags:

CHECK: Maybe invalid; must check parents before recomputing
DIRTY: Definitely invalid; must recompute
RUNNING: Currently recomputing (cycle / reentrancy guard)
EFFECT: Marks effects (non-memoized sink)

States do not use flags; they directly invalidate dependents on write.

⸻

## 3. Invalidation Semantics (Push Phase)

State Write

When a state value changes:
	1.	Direct subscribers are marked DIRTY
	2.	Their downstream dependents are marked CHECK (lazily)
	3.	Effects are queued and flushed immediately if not inside a batch

Computed Change

When a computed recomputes:
	•	If its value changed, dependents are marked DIRTY
	•	If unchanged, dependents remain CHECK or CLEAN

This allows avoiding unnecessary recomputation.

⸻

## 4. Lazy Evaluation Semantics (Pull Phase)

get(node)
	•	If node is not CHECK/DIRTY → return cached value
	•	Otherwise → update node

Updating a Computed (recursive, flag-driven)
	1.	CHECK flag
	•	Iterate dependencies
	•	Recursively update any CHECK/DIRTY parent
	•	If any parent recomputation marks self DIRTY → stop checking
	2.	DIRTY flag
	•	Recompute value
	•	Track dependencies dynamically
	•	Compare old vs new value
	•	Mark dependents DIRTY only if value changed
	•	Clear CHECK/DIRTY/RUNNING flags

This algorithm correctly handles diamond graphs and avoids redundant recomputation.

⸻

## 5. Dependency Tracking (Dynamic Graph)

Linked-Edge Model
	•	Each dependency is a Link object
	•	Links live simultaneously in:
	•	source.subsHead (subscribers, singly-linked)
	•	target.depsHead (dependencies)

Dynamic Dependencies

Dependencies may change on each recomputation (e.g. conditional branches).

Stable-Order Fast Path (implicit cursor)

Assumptions:
	•	Dependency sets are mostly stable
	•	Dependency order is usually stable

Mechanism:
	•	During recompute, depsTail is reset to null
	•	The implicit cursor is depsTail?.nextDep or depsHead
	•	If the next read matches the cursor → reuse the existing link (O(1))
	•	Otherwise → create a new link

This yields a very fast no-op path when dependencies are unchanged.

⸻

## 6. Duplicate Reads in a Single Run
Duplicate reads are not explicitly deduplicated in the current implementation. The stable-order fast path and linked-edge model keep costs low in the common case, but duplicate edges can occur if a node is read multiple times within the same run.

⸻

## 7. Dependency Rebuild Algorithm

Each recomputation performs an in-place rebuild:
	1.	Build phase
	•	Track dependencies as they are read
	•	Reuse existing links via the implicit cursor
	•	Append links to the existing dependency list tail
	2.	Finalize phase
	•	Sweep stale deps starting at depsTail.nextDep
	•	Unlink stale edges
	•	Terminate the deps list at depsTail

No edge pooling is required (GC handles unused links).

⸻

## 8. Tracking Context (Nested Evaluation)

Dependency tracking is dynamically scoped:
	•	When a node is evaluated, it becomes the current tracking target
	•	If evaluation triggers nested recomputation, the tracking context is temporarily replaced
	•	Context is restored on return

This ensures:
	•	Correct attribution of dependencies
	•	No accidental “flattening” of the graph

This is implemented via:
	•	a current target pointer
	•	restoration via the JavaScript call stack

⸻

## 9. Ownership and Lifetime Management

Tracking context ≠ ownership.

Ownership Graph

Ownership is explicit and build-scoped:
	•	A build scope collects created computeds/effects in an array
	•	Disposing a scope unlinks all edges and clears the array

This enables:
	•	Deterministic teardown
	•	No retained graph references
	•	Clear separation of concerns

⸻

## 10. Glitch-Freedom Guarantees

The system guarantees:
	•	Each node runs at most once per invalidation
	•	No node observes partially updated parents
	•	Diamond patterns are handled correctly
	•	Effects run only after all relevant dependencies are consistent

⸻

## 11. Design Goals Recap
	•	Plain objects for nodes and edges
	•	Detached logic (no methods on nodes)
	•	Minimal bookkeeping on hot paths
	•	Lazy, memoized evaluation
	•	Dynamic dependency sets
	•	Deterministic cleanup
	•	GC-friendly (no pooling required)

⸻

## 12. Deviations from the original spec (and why)

1) Single flag bitset instead of multi-field state
	• Reason: fewer branches and smaller object shapes in hot paths
	• Alternative considered: separate state fields and epochs (slower)

2) Implicit cursor rebuild (depsTail as cursor)
	• Reason: avoids per-computation cursor storage and extra scans
	• Alternative considered: explicit cursor and stable-order scan

3) No per-run dedup of duplicate reads
	• Reason: removes per-run markers and additional checks
	• Alternative considered: seenEpoch/per-link markers (more overhead)

4) Recursive, flag-driven updates (call stack ordering)
	• Reason: lower overhead than explicit two-phase stack
	• Alternative considered: iterative two-phase traversal (slower, needed reentrancy-safe stacks)

5) Single-linked subscriber lists
	• Reason: smaller link objects and fewer writes on hot paths
	• Alternative considered: doubly-linked subs (faster removals in cleanup, slower in hot path)

6) Flush only at write/batch boundaries
	• Reason: guarantees effects do not run mid-recompute without runDepth/isFlushing
	• Alternative considered: flush inside recompute with runDepth/isFlushing guards
