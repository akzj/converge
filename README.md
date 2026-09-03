# Converge

Converge is an embeddable Go reconciliation engine for Agent runtimes. It
accepts complete desired snapshots, builds generation-aware plans, executes
provider operations, and persists enough state to recover conservatively after
a process restart.

The module exposes one recommended entry package and three supporting packages:

- `converge`: recommended Agent integration boundary, including durable
  snapshot acceptance and status APIs;
- `core`: reconciliation, plans, attempts, external effects, recovery, and
  SQLite/in-memory storage implementations;
- `observability`: backend-neutral tracing and transition contracts plus a
  bounded asynchronous adapter;
- `pkg/model`: wire and lifecycle model types.

## Embedding

```go
ctx, cancel := context.WithCancel(context.Background())

runtime, err := converge.OpenSQLiteRuntime(
    ctx,
    "/var/lib/my-agent/converge.db",
    core.WithLogger(logger),
    core.WithObserver(observer),
)
if err != nil {
    return err
}

if err := runtime.RegisterProvider(ctx, provider); err != nil {
	_ = runtime.Close()
    return err
}

runErr := runtime.Run(ctx)
// Runtime.Run returns only after tracked planning, execution, control,
// deletion, verification, and outbox workers have stopped.
if err := runtime.Close(); err != nil {
    return err
}
return runErr
```

For normal shutdown, call `cancel` and wait for `Runtime.Run` to return before
closing the SQLite store or Provider resources. Providers must honor context
cancellation; Go cannot forcibly stop an implementation that ignores it.
`Runtime.Run` and `Reconciler.Run` are single-use and reject a second invocation.

`OpenSQLiteRuntime` owns its SQLite store. After `Run` returns, call
`Runtime.Close` to release it. The caller still owns Provider resources and any
`observability.AsyncObserver.Shutdown`.

Advanced users that need custom storage or event-bus wiring may use the `core`
package directly and assume its lifecycle invariants.

## Provider lifecycle

Register Providers before starting the runtime. A plan remains bound to the
Provider digest that created it, so rolling upgrades retain older Provider
instances while durable work may still reference them. When the runtime is
stopped, `UnregisterProviderVersion` safely removes an unused, non-current
version and rejects removal of a version still referenced by durable state.

## Durability and protocol

See [Runtime integration](docs/runtime.md),
[SQLite durability](docs/adr/0002-sqlite-durable-store.md),
[External Active Effects](docs/design/external-active-effects.md), and
[Observability](docs/design/observability.md).

The module currently targets Go 1.26.1. Until a semantic-version release is
tagged, consumers should pin an audited commit rather than follow `main`.
