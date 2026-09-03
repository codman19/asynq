# Design Doc: PreDequeueFunc — a Pre-Dequeue Extension Point

> Status: Confirmed, implemented
> Date: 2026-09-03
> Revision: 2026-09-03 — signature changed from returning `bool` to returning the list of queues to fetch from, enabling per-queue control (§3.2).

## 1. Background and Goals

When using asynq, we need to run some pre-checks **before a task is dequeued** (e.g. whether downstream dependencies are ready, whether system resources are sufficient, whether the preconditions of a certain kind of task are met, whether a global concurrency limit has been exceeded). When the conditions are not met, **do not fetch the task** — leave it untouched in the pending state.

All of asynq's existing extension points are located **after** the dequeue, which cannot satisfy this requirement. Hence a new pre-dequeue hook: `PreDequeueFunc`.

## 2. Current-State Analysis

### 2.1 The critical path of fetching a task

```
Server.Start() → processor.start() → a single processor goroutine looping p.exec()
```

`processor.exec()` (processor.go) is the only entry point for fetching tasks:

1. `case p.sema <- struct{}{}` — acquire a concurrency token (limit = `Config.Concurrency`);
2. `qnames := p.queues()` — compute the list of queues to query this round, by priority (with randomization / strict mode);
3. `p.broker.Dequeue(qnames...)` — the Redis Lua script `RPOPLPUSH` moves the task from pending to active and writes a lease.

### 2.2 Why existing mechanisms fall short

| Mechanism | Location | Problem |
|---|---|---|
| `ServeMux.Use` middleware | Inside the Handler chain, after Dequeue | The task is already in active holding a lease; returning an error goes through failure semantics (`retried+1`, failure stats, exponential-backoff retry) — the task cannot stay untouched |
| `ErrorHandler` | After processing fails | Same as above; after-the-fact notification |
| `IsFailure` / `RetryDelayFunc` | Fine-tuning failure semantics | Does not change the fact that the task has already been dequeued |
| `Inspector.PauseQueue` | Redis global state | Coarse-grained: affects all server instances and needs an external driver; cannot do per-round in-process checks |

## 3. API Design

### 3.1 Type and config field

Add a new field to `Config`, styled after `RetryDelayFunc` / `IsFailure` (type name equals field name):

```go
// PreDequeueFunc is called before the server attempts to fetch tasks
// from the given queues.
type PreDequeueFunc func(ctx context.Context, queues []string) []string
```

```go
type Config struct {
    // ...existing fields...

    // PreDequeueFunc optionally specifies a function that is called before
    // each fetch attempt. It returns the list of queues to fetch tasks from
    // in the current attempt. If the function returns an empty list, the
    // server skips fetching; tasks remain in the pending state untouched,
    // and the server retries after TaskCheckInterval.
    PreDequeueFunc PreDequeueFunc
}
```

### 3.2 Semantic contract

- **`queues` argument**: the list of queue names about to be queried this round (priority-randomized or strict order);
- **`ctx` argument**: a shutdown-aware context. **It is canceled when the server stops pulling tasks** (`Stop()` / `Shutdown()`), used to unblock waits inside the hook;
- **return value** = the list of queues to query this round:
  - return the input unchanged (or a subset of it) → fetch only from those queues, in the **returned order**. Returning a single name realizes "fetch from one queue this round"; returning a subset realizes "queue A's downstream is not ready, but queue B may be fetched";
  - return `nil` / empty → skip fetching this round; tasks stay in the pending state untouched (not moved to active, no lease, no failure record, no `retried` change); re-checked after backoff;
  - **names not present in the `queues` argument are dropped** (the hook can only narrow, never widen — a returned queue the server is not configured to serve must not be fetched);
  - **duplicate names are deduped**;
- **unset**: behavior is exactly the same as before (the overhead of a single nil check; zero allocation);
- **panic**: the framework recovers, treats it as returning an empty list and logs an error (aligned with how `perform` handles panics in user code); a panic must not take down the processor goroutine.

Why return a queue list rather than `bool`: the single `bool` can only express "fetch / don't fetch from **all** queues this round". With the list, per-queue pre-checks become possible — the hook decides which queues' conditions are met. Returning `queues` as-is is exactly equivalent to the old `return true`; returning `nil` is exactly equivalent to the old `return false`; so the old semantics remain expressible as special cases.

Note on efficiency: returning a subset (even a single queue) still keeps the atomic multi-queue `Dequeue` path — the filtered list is passed to the same Lua script in one round trip. This is deliberately different from "make `processor.queues()` itself return one name": a lone random pick that lands on an empty queue would trigger the empty-queue sleep even when other (allowed) queues have pending tasks, hurting throughput.

### 3.3 Blocking behavior contract

The hook runs on the single processor goroutine; **bounded blocking** is allowed (e.g. `select` on a condition channel / a `time.After` timeout / `ctx.Done()`):

- while the hook is blocked, all queues of this server instance stop being fetched (equivalent to a global pause, at instance granularity);
- every blocking path must have an exit. Recommended pattern:

```go
PreDequeueFunc: func(ctx context.Context, queues []string) []string {
    select {
    case <-depsReady:                    // condition met, allow immediately (faster reaction than fixed backoff)
        return queues
    case <-time.After(2 * time.Second):  // upper bound of the bounded wait
        return nil
    case <-ctx.Done():                   // exit immediately when the server stops
        return nil
    }
}
```

- **Unbounded blocking hangs Shutdown** (the `p.done <-` send in `stop()` and the token drain in `shutdown()` both wait for the hook to return; the `ShutdownTimeout`/abort mechanism only applies to in-flight workers and cannot rescue the processor goroutine). The framework therefore provides the `ctx` cancel as a safety net for well-written hooks.

## 4. Implementation Changes

### 4.1 server.go

1. Define the `PreDequeueFunc` type (with full doc comments);
2. Add the `PreDequeueFunc` field to `Config`;
3. Pass it through in `NewServerFromRedisClient` to `processorParams.preDequeueFunc`.

### 4.2 processor.go

1. Add fields to `processorParams` / `processor`:

```go
preDequeueFunc PreDequeueFunc

// preDequeueCtx is passed to preDequeueFunc and canceled when the
// processor stops, to unblock a function that is waiting.
preDequeueCtx    context.Context
preDequeueCancel context.CancelFunc
```

2. Create them in `newProcessor`: `ctx, cancel := context.WithCancel(context.Background())`;
3. Call `preDequeueCancel()` in `stop()` before sending on `done`;
4. Insert the filter in `exec()` before `Dequeue`:

```go
qnames := p.queues()
qnames = p.filterQueues(qnames)
if len(qnames) == 0 {
    // Conditions are not met; skip fetching tasks this round.
    // Wait before the next attempt to avoid slamming redis.
    p.waitBeforeNextAttempt()
    <-p.sema // release token
    return
}
msg, leaseExpirationTime, err := p.broker.Dequeue(qnames...)
```

5. The `filterQueues` wrapper (nil short-circuit + validation + panic recovery):

```go
// filterQueues applies the user-configured PreDequeueFunc to the given
// queue names and returns the list of queues to fetch tasks from in the
// current attempt. Queue names not present in qnames are dropped, and
// duplicate names are deduped. A nil return value skips the current
// fetch attempt.
func (p *processor) filterQueues(qnames []string) (queues []string) {
    if p.preDequeueFunc == nil {
        return qnames
    }
    defer func() {
        if x := recover(); x != nil {
            p.logger.Errorf("Panic in PreDequeueFunc, skipping fetch: %v ...", x)
            queues = nil
        }
    }()
    allowed := make(map[string]struct{}, len(qnames))
    for _, q := range qnames {
        allowed[q] = struct{}{}
    }
    for _, q := range p.preDequeueFunc(p.preDequeueCtx, qnames) {
        if _, ok := allowed[q]; ok {
            queues = append(queues, q)
            delete(allowed, q) // dedupe
        }
    }
    return queues
}
```

6. `waitBeforeNextAttempt`: the same backoff as the "all queues empty" path (`[0.5×, 1.5×) × taskCheckInterval` + random jitter), but implemented with a `select` that also watches `preDequeueCtx.Done()`, so shutdown is never delayed by the backoff sleep.

## 5. Decision Record

| Decision point | Conclusion | Rationale |
|---|---|---|
| Signature | `(ctx, queues) []string` — returns the queues to fetch from | `bool` can only gate all queues at once; the list enables per-queue pre-checks while remaining backward-expressive (`queues` ≡ old `true`, `nil` ≡ old `false`) |
| Unknown names in return value | Dropped (intersection with the input `queues`) | The hook may only narrow: a queue the server is not configured to serve must not be fetched by it |
| Order of the returned list | The returned order is used as-is (after dropping/dedup) | Predictable contract; a hook that iterates the input naturally preserves the priority order |
| Duplicates in return value | Deduped | One queue must not be checked twice in a single atomic Dequeue |
| Backoff after empty result | Reuse `TaskCheckInterval` + jitter | Consistent with the empty-queue path; avoids hot-looping Redis |
| Token during empty result | Released after the sleep (aligned with the empty-queue path) | Behavioral consistency |
| Hook ctx | Derived from `context.Background()`, canceled in `stop()` | `BaseContext` has no shutdown semantics and cannot be reused; the cancel guarantees a blocking hook can exit during shutdown |
| Panic handling | recover + treat as empty result + error log | Aligned with `perform`; the processor goroutine must not be taken down by user code |
| Alternative: make `processor.queues()` return one name per call | Rejected | A lone random pick landing on an empty queue sleeps even when other queues have tasks; the subset return keeps the atomic multi-queue Dequeue (one round trip) |
| Alternative: injectable custom Broker | Rejected | Would require exposing `internal/base.Broker`, breaking the internal boundary; large change surface |

## 6. Test Plan

Reuse the existing infrastructure in `processor_test.go` (real Redis db14, `h.SeedPendingQueue` seeding, setting unexported fields directly, `p.start` / `p.shutdown` lifecycle). Tests set `taskCheckInterval` small (~50ms) so deny-path cases run at millisecond speed.

| # | Case | Assertion |
|---|---|---|
| 1 | Hook unset | All existing tests pass without regression |
| 2 | Hook returns `queues` unchanged | Tasks processed as usual; active drained |
| 3 | Hook returns `nil` | Handler never called; tasks still in pending; active empty |
| 4 | skip→allow transition | Task eventually processed; hook call count reaches the threshold (polling recovers) |
| 5 | `queues` argument (non-strict, multi-queue) | Every sample contains all configured queues (set equality, order random) |
| 6 | `queues` argument (`StrictPriority`) | Every sample strictly equals the priority-descending order |
| 7 | Hook panics | Processor does not crash; task not fetched during panicking rounds; processed normally after recovery |
| 8 | Hook blocks until `ctx.Done()` + shutdown | `shutdown()` returns within the deadline (verifies `preDequeueCancel` works; no hang) |
| 9 | Server-level wiring | `NewServer` + `Config.PreDequeueFunc` → `srv.processor.preDequeueFunc` is non-nil |
| 10 | `filterQueues` unit test | nil-func passthrough; subset in returned order; unknown names dropped; duplicates deduped; panic → `nil` |
| 11 | Subset selection (integration, two queues) | Hook keeps returning only `"default"` → all `default` tasks processed, `critical` tasks still pending, active empty |

## 7. Usage Example

```go
srv := asynq.NewServer(redisOpt, asynq.Config{
    Concurrency: 10,
    Queues: map[string]int{
        "critical": 6,
        "default":  3,
        "crawl":    1,
    },
    PreDequeueFunc: func(ctx context.Context, queues []string) []string {
        var allowed []string
        for _, q := range queues {
            switch q {
            case "crawl":
                // Example: fetch from "crawl" only while the disk has headroom.
                if disk.Usage() < 0.9 {
                    allowed = append(allowed, q)
                }
            default:
                // Example: gate every other queue on downstream readiness.
                if deps.Ready() {
                    allowed = append(allowed, q)
                }
            }
        }
        return allowed // possibly nil — this round is skipped entirely
    },
})
```
