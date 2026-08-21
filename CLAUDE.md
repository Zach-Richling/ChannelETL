# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
dotnet build                                          # build the solution
dotnet test                                            # run all tests
dotnet test --filter "FullyQualifiedName~PipelineTests"  # run a single test class
dotnet test --filter "FullyQualifiedName~CompletesAndPreservesOrder"  # run a single test
dotnet test -- --coverage --coverage-settings ChannelETL.Tests/CodeCoverage.config --coverage-output-format cobertura --coverage-output coverage.cobertura.xml  # run with code coverage (report lands in TestResults/)
```

Target framework is `net10.0`. Test project uses xunit.v3 (via `Microsoft.Testing.Platform`, not the legacy VSTest host — `dotnet test` requires the `test.runner` setting in `global.json`) with NSubstitute for mocking. Code coverage is collected with `Microsoft.Testing.Extensions.CodeCoverage`; `ChannelETL.Tests/CodeCoverage.config` explicitly excludes the test assembly itself from the report.

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

### Tests

`ChannelETL.Tests/TestComponents.cs` provides minimal `Source<T>`/`Transform<TIn,TOut>`/`Destination<T>` fakes for constructing pipelines in tests without mocking frameworks; use these (via `TestComponents.CreateTestSource/CreateTestTransform/CreateTestDestination`) before reaching for NSubstitute. Tests define a concrete `TestPipeline : Pipeline<int, string>` per test class since `Pipeline<,>` is abstract.

## Conventions

- Commit messages must follow Conventional Commits (see `.github/copilot-instructions.md`).
