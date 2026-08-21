# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
dotnet build                                          # build the solution
dotnet test                                            # run all tests (unit + integration; needs Docker)
dotnet test ChannelETL.Tests                           # unit tests only, no Docker needed
dotnet test --filter "FullyQualifiedName~PipelineTests"  # run a single test class
dotnet test --filter "FullyQualifiedName~CompletesAndPreservesOrder"  # run a single test
dotnet test -- --coverage --coverage-settings CodeCoverage.config --coverage-output-format cobertura  # code coverage (one report per test project in TestResults/)
```

Target framework is `net10.0`. Test projects use xunit.v3 (via `Microsoft.Testing.Platform`, not the legacy VSTest host — `dotnet test` requires the `test.runner` setting in `global.json`) with NSubstitute for mocking.

Code coverage is collected with `Microsoft.Testing.Extensions.CodeCoverage`, referenced by both test projects; the root `CodeCoverage.config` excludes both test assemblies from the report. Do **not** pass `--coverage-output` when running the whole solution — both test projects would write to that same filename and one would clobber the other. Omitting it yields a uniquely-named report per project; union them to get the combined figure.

## Architecture

ChannelETL is a small library for building async ETL pipelines on top of `System.Threading.Channels`, with dependency-graph orchestration across multiple pipelines. All public types live in the `ChannelETL` namespace regardless of folder (see `GlobalSuppressions.cs`).

### Pipeline (single source → transform → destination)

A `Pipeline<TSource, TDestination>` (`ChannelETL/Pipeline/Pipeline.cs`) wires together three user-supplied pieces:
- `IPipelineSource<TSource>.ProduceAsync` — an `IAsyncEnumerable<TSource>` producer
- `IPipelineTransformation<TSource, TDest>.TransformAsync` — one-item-at-a-time transform
- `IPipelineDestination<TDest>` — `ConsumeAsync` per item plus `CompleteAsync` for flush/cleanup

Internally `RunAsync` runs produce/transform/consume concurrently as three tasks connected by two bounded `Channel<T>`s (capacity 100, single reader/writer, `Wait` on full). Order is preserved because each stage is single-threaded end-to-end. Failure in any stage is caught, logged, and downgrades the outcome to `PipelineOutcome.Failure`; a canceled `PipelineExecutionContext.Token` (or an upstream parent pipeline that didn't succeed) produces `PipelineOutcome.Canceled`. Completion is exposed via `CompletionTask` (backed by a `TaskCompletionSource`) so other pipelines can await it — see "Parent pipelines" below. Errors in the destination's `ConsumeAsync` are held and re-thrown after `CompleteAsync` still runs, and if both throw they're combined into an `AggregateException`.

`BatchedPipelineDestination<TDest>` (`ChannelETL/Destination/BatchedPipelineDestination.cs`) is a helper base class for destinations that write in batches: it buffers `ConsumeAsync` calls and invokes the abstract `ConsumeBatchAsync(IReadOnlyList<TDest>, token)` once the batch size is reached or on `CompleteAsync`.

### Parent pipelines (cross-pipeline dependencies)

A pipeline can declare parent pipelines it must wait on via `PipelineExecutionContext.ParentPipelines`. `RunAsync` awaits all parents' `CompletionTask`s before doing any work; if the token is already canceled or any parent didn't succeed, the pipeline short-circuits to `PipelineOutcome.Canceled` without touching its own source/transform/destination.

### PipelineGroup (orchestrating many pipelines with dependencies)

`PipelineGroup` (`ChannelETL/PipelineGroup/PipelineGroup.cs`) is the entry point for running a set of related pipelines together. Subclasses register pipeline types in their constructor via `AddPipeline<TPipeline>()`, which returns an `IPipelineBuilder` for declaring dependencies with `.WaitFor<TParentPipeline>()` (throws if the parent type wasn't also added to the group, or if a dependency is declared twice).

At `RunAsync`, the group:
1. Creates a fresh DI scope (`IServiceScopeFactory` from `PipelineGroupExecutionContext`) so each group run gets its own pipeline instances
2. Resolves every registered pipeline type and its `ILogger<TPipeline>` from that scope
3. Builds each pipeline's `ParentPipelines` from the recorded builder dependencies and starts all pipelines concurrently with `Task.WhenAll`
4. Catches and logs any exceptions at the group level (pipelines themselves shouldn't normally throw out of `RunAsync` — see error handling above)

Because dependencies are resolved by type against pipelines registered in the *same* group, pipeline classes must be added to DI (see below) and every `WaitFor<T>` target must have been added to the same group first.

### DI registration

`IServiceCollectionExtensions.AddPipelinesFromAssembly(assembly)` scans an assembly for concrete classes implementing `IPipeline`, `IPipelineGroup`, `IPipelineSource<>`, `IPipelineTransformation<,>`, or `IPipelineDestination<>`, and registers each as `Scoped` under its concrete type and all its interfaces. `ChannelETL.csproj` grants `InternalsVisibleTo` to `ChannelETL.Tests`.

### Adapters

`ChannelETL.Adapters.Dapper` holds provider-specific implementations of the core interfaces. `DapperPipelineSource<TSource>` (`Source/DapperPipelineSource.cs`) is an `IPipelineSource<TSource>` that streams rows from a `DbConnection` via Dapper's `QueryUnbufferedAsync`. Subclasses supply the query through `protected init` properties — `CommandType` (default `Text`) plus either `Text` or `StoredProcedureName`, and optional `Parameters`. The `protected Sql` property resolves which of those to use, throwing `ArgumentException` if the one matching the `CommandType` is missing and `NotSupportedException` for any `CommandType` other than `Text` or `StoredProcedure` (`TableDirect` included — no common ADO.NET provider implements it).

`Sql` is resolved lazily: `ProduceAsync` is an async iterator, so a misconfigured source does not throw until enumeration begins. Cancellation is applied with `.WithCancellation(token)` on the Dapper enumerable, so the token reaches the underlying reader rather than only being checked between yielded rows.

### Tests

Two test projects, both using xunit.v3. Test file paths mirror the source file they cover (`ChannelETL/Destination/BatchedPipelineDestination.cs` → `ChannelETL.Tests/Destination/BatchedPipelineDestinationTests.cs`), with the adapter's project name becoming a folder segment (`ChannelETL.Tests/Adapters/Dapper/Source/...`). Every file keeps the project's flat root namespace regardless of folder — see each project's `GlobalSuppressions.cs`.

- **`ChannelETL.Tests`** — unit tests, no external dependencies. Prefer NSubstitute over hand-written fakes. `TestHelpers.cs` holds only genuinely shared helpers (currently `CreateContext` for `PipelineExecutionContext`); helpers used by a single file stay in that file. Because `Pipeline<,>` and `DapperPipelineSource<>` are abstract with protected members, test classes define small concrete subclasses that assign the `init` properties from their own constructor — an object initializer at the call site cannot reach protected members.
- **`ChannelETL.IntegrationTests`** — requires Docker. `SqlServerFixture` starts one pinned SQL Server container per run via Testcontainers and seeds `dbo.Orders` plus two stored procedures; test classes join `SqlServerCollection` to share it. These cover what unit tests structurally cannot: `QueryUnbufferedAsync` is a static extension method with no seam for a test double, so the streaming loop in `ProduceAsync` only executes against a real provider.

## Conventions

- Commit messages must follow Conventional Commits (see `.github/copilot-instructions.md`).
