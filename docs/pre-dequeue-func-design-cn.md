# 设计文档：PreDequeueFunc —— 任务取出前的扩展点

> 状态：已确认，已实现
> 日期：2026-09-03

## 1. 背景与目标

在使用 asynq 时，需要在**任务被取出（Dequeue）之前**执行一些预先检查（如依赖服务是否就绪、系统资源是否充足、某类任务的前置条件是否满足、是否超过全局并发限额等）。当条件不满足时，**不取出任务**，让任务原封不动地留在 pending 状态。

asynq 现有的扩展点全部位于任务取出**之后**，无法满足该需求，因此新增一个取出前的钩子：`PreDequeueFunc`。

## 2. 现状分析

### 2.1 任务取出的关键路径

```
Server.Start() → processor.start() → 单一 processor 协程循环调 p.exec()
```

`processor.exec()`（processor.go）是唯一的取任务入口：

1. `case p.sema <- struct{}{}` —— 获取并发令牌（上限 = `Config.Concurrency`）；
2. `qnames := p.queues()` —— 按优先级（含随机化 / 严格模式）算出本轮要查询的队列列表；
3. `p.broker.Dequeue(qnames...)` —— Redis Lua 脚本 `RPOPLPUSH` 将任务从 pending 移入 active，并写入租约（lease）。

### 2.2 为什么现有机制不满足

| 机制 | 位置 | 问题 |
|---|---|---|
| `ServeMux.Use` middleware | Handler 链内，Dequeue 之后 | 任务已进入 active 并持有 lease；返回错误会走失败语义（`retried+1`、失败统计、指数退避重试），无法做到"任务原地不动" |
| `ErrorHandler` | 处理失败后 | 同上，事后通知 |
| `IsFailure` / `RetryDelayFunc` | 失败语义微调 | 不改变"任务已被取出"的事实 |
| `Inspector.PauseQueue` | Redis 全局状态 | 粗粒度：影响所有 server 实例、需要外部驱动，无法做进程内逐轮检查 |

## 3. API 设计

### 3.1 类型与配置字段

在 `Config` 中新增字段，风格对齐 `RetryDelayFunc` / `IsFailure`（类型即字段名）：

```go
// PreDequeueFunc is called before the server attempts to fetch tasks
// from the given queues.
type PreDequeueFunc func(ctx context.Context, queues []string) bool
```

```go
type Config struct {
    // ...existing fields...

    // PreDequeueFunc optionally specifies a function that is called before
    // each fetch attempt. If it returns false, the server skips fetching
    // tasks; tasks remain in the pending state untouched, and the server
    // retries after TaskCheckInterval.
    PreDequeueFunc PreDequeueFunc
}
```

### 3.2 语义契约

- **入参 `queues`**：本轮将要查询的队列名列表（按优先级随机或严格排序），支持"只对某个队列做检查"的用法；
- **入参 `ctx`**：shutdown-aware context。**server 停止取任务时（`Stop()` / `Shutdown()`）会被 cancel**，用于解除钩子内的阻塞等待；
- **返回 `true`**：继续本轮取任务；
- **返回 `false`**：跳过本轮取任务，任务留在 pending 状态不动（不入 active、无 lease、无失败记录、无 retried 变化），退避后重查；
- **未设置**：行为与原来完全一致（仅一次 nil 判断开销）；
- **panic**：框架 recover，视为返回 `false` 并记录错误日志（对齐 `perform` 对用户代码 panic 的处理），不得击穿 processor 协程。

### 3.3 阻塞行为约定

钩子运行在唯一的 processor 协程上，允许**有界阻塞**（如 `select` 等待条件 channel / `time.After` 超时 / `ctx.Done()`）：

- 阻塞期间该 server 实例的所有队列停止取任务（等效全局暂停，粒度是本实例）；
- 钩子必须保证每条阻塞路径有出口。推荐模式：

```go
PreDequeueFunc: func(ctx context.Context, queues []string) bool {
    select {
    case <-depsReady:                 // 条件就绪，立即放行（反应快于固定退避）
        return true
    case <-time.After(2 * time.Second): // 有界等待上限
        return false
    case <-ctx.Done():                // server 停止时立即退出
        return false
    }
}
```

- **无界阻塞会挂死 Shutdown**（`stop()` 中 `p.done <-` 与 `shutdown()` 中令牌排水都会等钩子返回；`ShutdownTimeout`/abort 机制只作用于 in-flight worker，救不了 processor 协程），因此框架侧提供 `ctx` cancel 来兜底规范编写的钩子。

## 4. 实现改动

### 4.1 server.go

1. 定义 `PreDequeueFunc` 类型（含完整文档注释）；
2. `Config` 新增 `PreDequeueFunc` 字段；
3. `NewServerFromRedisClient` 中透传至 `processorParams.preDequeueFunc`。

### 4.2 processor.go

1. `processorParams` / `processor` 新增字段：

```go
preDequeueFunc PreDequeueFunc

// preDequeueCtx is passed to PreDequeueFunc and canceled when the
// processor stops, to unblock a function that is waiting.
preDequeueCtx    context.Context
preDequeueCancel context.CancelFunc
```

2. `newProcessor` 中创建：`ctx, cancel := context.WithCancel(context.Background())`；
3. `stop()` 中在发送 `done` 之前调用 `preDequeueCancel()`；
4. `exec()` 在 `Dequeue` 之前插入检查：

```go
qnames := p.queues()
if !p.allowDequeue(qnames) {
    // Conditions are not met; skip fetching tasks this round.
    // Wait before the next attempt to avoid slamming redis.
    p.waitBeforeNextAttempt()
    <-p.sema // release token
    return
}
msg, leaseExpirationTime, err := p.broker.Dequeue(qnames...)
```

5. `allowDequeue` 包装（nil 短路 + panic 恢复）：

```go
func (p *processor) allowDequeue(qnames []string) (ok bool) {
    if p.preDequeueFunc == nil {
        return true
    }
    defer func() {
        if x := recover(); x != nil {
            p.logger.Errorf("Panic in PreDequeueFunc, skipping fetch: %v ...", x)
            ok = false
        }
    }()
    return p.preDequeueFunc(p.preDequeueCtx, qnames)
}
```

6. `waitBeforeNextAttempt`：与"队列全空"路径相同的退避（`[0.5×, 1.5×) × taskCheckInterval` + 随机 jitter），但用 `select` 同时监听 `preDequeueCtx.Done()`，保证 shutdown 时不被退避睡眠拖住。

## 5. 行为细节决策记录

| 决策点 | 结论 | 理由 |
|---|---|---|
| 签名 | `(ctx, queues) bool` | 保持最小 API；deny 原因日志由用户闭包自打（与 `IsFailure` 一致） |
| deny 后退避 | 复用 `TaskCheckInterval` + jitter | 与空队列路径一致，避免热循环压垮 Redis |
| deny 期间令牌 | sleep 后释放（对齐空队列路径） | 行为一致性 |
| 钩子 ctx | `context.Background()` 派生，`stop()` 时 cancel | `BaseContext` 无关闭语义，不能复用；cancel 保证阻塞钩子在 shutdown 时可退出 |
| panic 处理 | recover + 视为 deny + 错误日志 | 对齐 `perform`；processor 协程不可被用户代码击穿 |
| 备选：注入自定义 Broker | 否决 | 需暴露 `internal/base.Broker`，破坏 internal 边界，改动面大 |

## 6. 测试方案

沿用 `processor_test.go` 现有基建（真实 Redis db14、`h.SeedPendingQueue` 造数、直接设置未导出字段、`p.start` / `p.shutdown` 启停）。测试中将 `taskCheckInterval` 调小（~50ms）使 deny 路径用例跑到毫秒级。

| # | 用例 | 断言 |
|---|---|---|
| 1 | 未设置钩子 | 现有全部测试无回归 |
| 2 | 钩子恒 `true` | 任务照常处理完，active 清空 |
| 3 | 钩子恒 `false` | handler 未被调用；任务仍在 pending；active 为空 |
| 4 | deny→allow 转换 | 任务最终被处理；钩子调用次数达到阈值（轮询可恢复） |
| 5 | `queues` 参数（非 strict 多队列） | 每次采样均包含全部配置队列（集合相等，顺序随机） |
| 6 | `queues` 参数（`StrictPriority`） | 每次采样顺序严格等于优先级降序 |
| 7 | 钩子 panic | processor 不崩；panic 期间任务未被取出；恢复后正常处理 |
| 8 | 钩子阻塞至 `ctx.Done()` + shutdown | `shutdown()` 在时限内返回（验证 `preDequeueCancel` 生效，不挂死） |
| 9 | server 级透传 | `NewServer` + `Config.PreDequeueFunc` → `srv.processor.preDequeueFunc` 非空 |

## 7. 使用示例

```go
srv := asynq.NewServer(redisOpt, asynq.Config{
    Concurrency: 10,
    PreDequeueFunc: func(ctx context.Context, queues []string) bool {
        // 例：下游依赖未就绪时，不取出任何任务
        if !deps.Ready() {
            return false
        }
        // 例：只对 "crawl" 队列做磁盘水位检查
        for _, q := range queues {
            if q == "crawl" && disk.Usage() > 0.9 {
                return false
            }
        }
        return true
    },
})
```
