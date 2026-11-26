# Dataflow simulation engine

[![C++ CI](https://github.com/GraumanVA/dataflow/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/GraumanVA/dataflow/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![C++ standard: C++20](https://img.shields.io/badge/C%2B%2B-20-blue)](CMakeLists.txt)

This project runs configured acyclic dataflow graphs. Queues connect typed worker
ports, workers execute one transaction at a time, and observer workers can stop a
simulation after collecting enough statistics.

## Build and run

Use a Conan 2 profile for your compiler and install dependencies for each build
configuration you intend to use. For example, with a Visual Studio profile as
your Conan default, prepare both Debug and Release in the same build directory:

```text
conan install . -s:h build_type=Release -s:h compiler.cppstd=20 --build=missing
conan install . -s:h build_type=Debug -s:h compiler.cppstd=20 --build=missing
cmake --preset conan-default
cmake --build --preset conan-debug
ctest --preset conan-debug --output-on-failure
```

Conan also installs GoogleTest for the test targets. Use `conan-release` to
build or test Release. Run one scenario with:

```text
dataflow path/to/scenario.yaml
```

A complete extension example is provided in `examples/`. After building, run:

```text
dataflow_basic_example examples/basic/basic_scenario.yaml
```

A second small example defines a templated random-bit source and binary
symmetric channel, registers their `std::int8_t` specializations, and runs the
built-in BER observer over a crossover-probability sweep:

```text
dataflow_bsc_example examples/bsc_channel/bsc_sweep.yaml
```

The source exposes `seed`, `emitted_block_size`, and `probability_one`. The BSC
exposes `seed`, `accepted_size`, and `crossover_probability`. The engine
derives a separate random stream for each node, sweep point, and replica from
the configured base seed. The streams repeat for the same configuration and
standard library. Time-based statistics publication can still change the final
sample count and BER. See
`examples/bsc_channel/bsc_workers.h` for the worker definitions and
`examples/bsc_channel/bsc_sweep.yaml` for the complete graph.

The coordinator creates independent graph replicas according to
`runtime.threads`. Every replica runs in its own `std::jthread`; its graph,
workers, queues, and observer accumulators are thread-confined and need no
locking. At safe sweep boundaries, each replica publishes immutable observer
copies to a synchronized registry. The coordinator merges those copies for
stopping, live reporting, and the final report.

Results are written beneath `statistics/<scenario>/stats.csv`. Existing reports
are preserved; later runs create `stats_2.csv`, `stats_3.csv`, and so on. Sweep
results use one directory per point:
`statistics/<scenario>/case_0000/stats.csv`, `case_0001/stats.csv`, and so on.
With named sweep directories enabled, parameter labels and values are appended
to these stable indices.

## Scenario configuration

A typical scenario can look like this.

```yaml
scenario: awgn_ber

runtime:
  threads: 4
  statistics:
    publish_interval_seconds: 0.25
  stop:
    policy: all                 # any or all
    observers: [ber]
  reporting:
    terminal:
      enabled: true
      interval_seconds: 2
      update_in_place: true

nodes:
  - id: fanout
    type: broadcast_u8
    params:
      transaction_size: 256
      delivery: [reliable, best_effort]

  - id: ber
    type: ber_u8
    params:
      transaction_size: 256
      stop_mode: bit_errors     # or confidence_interval
      target_bit_errors: 1000

# Add application source/transform/sink nodes and connect every port in `edges`.
edges: []
```

`scenario` must be one folder name instead of full path. Reliable broadcaster outputs
apply backpressure. A best-effort output skips a complete transaction when its
queue lacks space.

Edges normally refer to ports by name and omit `type`; graph compilation infers
the queue type from the two port declarations:

```yaml
edges:
  - producer: {node: source, port: samples}
    consumer: {node: fanout, port: input}
```

Numeric ports and an explicit edge `type` remain accepted for compatibility.
An explicit type is treated as an assertion and must match both ports.
Graph validation failures throw `dataflow::GraphCompilationError`, available
through `<dataflow/graph_error.h>` and included by `<dataflow/engine.h>`. Its
`diagnostics()` collection contains the individual actionable errors.
Unknown fields are rejected throughout the scenario, including runtime,
node, edge, endpoint, and worker parameter mappings, so misspelled settings do
not silently fall back to defaults.

### Default worker parameters

The registry value-initializes a worker's `Config` and then overlays the YAML
fields. Default member initializers are therefore used when the corresponding
field is absent. Parsing remains strict for fields that are present, and
unknown fields are still rejected. `validate()` runs on the final configuration
after defaults have been applied:

```cpp
struct GainConfig {
    std::size_t transaction_size = 1;
    float gain = 1.0F;
};
```

Consequently, an empty `params: {}` selects both defaults. Config types must be
default-initializable. A member without an explicit initializer is still
value-initialized (for example, an integer becomes zero), so `validate()` should
reject that sentinel when the field is logically required. This is convenient,
but it also means newly added members can silently affect old scenarios; choose
defaults deliberately.

The BER observer defaults to bit-error stopping with
`target_bit_errors: 100`. Its confidence-interval settings are only validated
when `stop_mode: confidence_interval` is selected. A bit-error configuration can
therefore specify only what it needs:

```yaml
params:
  transaction_size: 256
  target_bit_errors: 1000
```

By default BER treats all bits in each integral element as payload. Set
`bits_per_element` to a smaller low-bit count for unpacked representations; the
BSC example uses `bits_per_element: 1` because every `std::int8_t` element holds
one logical bit. Zero retains the full storage width. In `bit_errors` stop mode,
the observer reports only `bits`, `bit_errors`, and `ber`; confidence bounds are
computed and displayed only in `confidence_interval` mode.

### Global parameter sweeps

A top-level `sweep` repeats a complete simulation point after the previous
point finishes normally. Every point still creates `runtime.threads` replicas,
and observer results are merged only among replicas of that point.

Each ordinary axis has a public `name`, one or more worker parameter `targets`,
and either an explicit list or a range. A target uses `param` for one field or
`path` for a nested field path:

```yaml
sweep:
  combine: product
  case_naming: parameters
  axes:
    - name: snr_db
      display_name: snr
      values: [-2.0, 0.0, 2.0]
      targets:
        - {node: channel, param: snr_db}

    - name: frame_size
      values:
        range: {start: 128, stop: 512, step: 128}
      targets:
        - {node: source, param: frame_size}
        - {node: channel, param: frame_size}

  derived:
    - name: case_id
      values:
        serial: {start: 0, step: 1}
      targets:
        - {node: source, path: [metadata, case_id]}

  replica_parameters:
    - name: seed
      values:
        serial: {start: 0, step: 1}
      targets:
        - {node: source, param: seed}
```

`combine: product` forms the Cartesian product; the last declared axis changes
fastest. `combine: zip` pairs values by position and requires equal axis
lengths. `combine` may be omitted when there is zero or one ordinary axis.

`display_name` is optional and defaults to `name`. It is used in terminal table
headings, command-line summaries, and named case directories, while `name`
remains the stable programmatic identifier in `SweepResult`. With
`case_naming: parameters`, directories retain their collision-free point index
and append sanitized labels and values, for example
`case_0000__snr=-2.0`. The default `case_naming: index` retains `case_0000`.
Live terminal updates replace only the current point's previous table; a
completed point remains visible when the next sweep point starts.

A range requires `start`, `step`, and exactly one of:

- `stop`, an inclusive bound when reached by the step; or
- `count`, the exact number of generated values.

Integer ranges remain integers. Any floating input produces floating values.
Ranges, expanded sweeps, empty value lists, zero steps, inconsistent zip
lengths, duplicate names, duplicate targets, and unknown sweep fields are
validated before execution. Expansion is limited to one million values or
points.

`derived` serial values advance once per expanded point, after product or zip
combination. `replica_parameters` advance by replica index and restart at the
same configured `start` for every point. With `runtime.threads: 4`, the example
assigns explicit seed parameters `0`, `1`, `2`, and `3` at every SNR. Workers
that use the engine's random stream helper can share a base seed across all
replicas. Replica-derived values may change worker behavior but must not change
port declarations, queue layout, or graph topology.

The command-line application detects sweeps automatically. Library callers use
`Engine::run_sweep()` and receive a `SweepResult` containing each point's named
parameters, each replica's derived parameters, `RunResult`, and statistics
file. Calling `Engine::run()` on a scenario that contains a sweep is rejected
to avoid accidentally running only one point. Calling `run_sweep()` for an
ordinary scenario returns a single point.

## Registering a worker

Include `<dataflow/worker.h>` and `<dataflow/worker_registry.h>`. For the common
case of a runtime number of homogeneous input and output ports, derive a small
worker from `WorkerFacade<Config, Input, Output>`. The facade owns the immutable
port layout, binds queues, and checks queue readiness; the derived worker only
owns its configuration and algorithm state. The registry parses YAML once and
reuses the immutable configuration and port layout for every replica. A static
`validate` function is optional and is called after parsing when present:

```cpp
struct GainConfig {
    std::size_t transaction_size = 1;
    float gain = 1.0F;
};

class Gain final : public dataflow::WorkerFacade<GainConfig, float, float> {
public:
    static void validate(const GainConfig& config) {
        if (config.transaction_size == 0)
            throw std::invalid_argument("transaction_size must be positive");
        if (!std::isfinite(config.gain))
            throw std::invalid_argument("gain must be finite");
    }

    static dataflow::WorkerPorts ports(const GainConfig& config) {
        return {{dataflow::port<float>("input", config.transaction_size)},
                {dataflow::port<float>("output", config.transaction_size)}};
    }

    explicit Gain(const GainConfig& config) : gain_(config.gain) {}

protected:
    void work_one(std::span<input_view> inputs,
                  std::span<output_view> outputs) override {
        for (std::size_t i = 0; i < inputs[0].size(); ++i)
            outputs[0][i] = inputs[0][i] * gain_;
    }

private:
    float gain_;
};
```

Sources and sinks can use the shorter `SourceFacade<Config, Output>` and
`SinkFacade<Config, Input>` aliases, so they do not name an unused scalar type.

Register its YAML type in the appropriate worker registry:

```cpp
workers.register_worker<Gain>("gain");
```

Applications should keep their registrations in an application-owned function
rather than editing the built-in registry. The high-level API owns the standard
registry and runtime dependencies:

```cpp
dataflow::Engine engine;
register_application_workers(engine.workers());
engine.run("scenario.yaml");
```

The node's `id` is its instance identifier; it is not duplicated inside worker
parameters. The engine reserves transaction and view storage during binding.
`work_one` therefore incurs no framework allocation and should process exactly
one transaction. A stateful source can optionally provide `can_process() const`;
the facade's final `is_ready()` combines that local condition with queue
readiness:

```cpp
protected:
[[nodiscard]] bool can_process() const noexcept override {
    return emitted_ < config_.transactions;
}
```

`SizedTypedWorker<Input, Output>` remains available as the advanced homogeneous
worker API when an implementation needs direct control over binding or
readiness. Such implementations use the original constructor taking
`std::shared_ptr<const WorkerPorts>` and override the virtual worker operations.

### Workers with heterogeneous ports

`SizedTypedWorker<Input, Output>` supports any runtime number of inputs with
one input type and outputs with one output type. For a fixed set of ports whose
individual types differ, use
`TypedWorkerFacade<Config, InputPorts<...>, OutputPorts<...>>`. Its views are
tuples ordered exactly like the layout returned by `ports()`:

```cpp
class Mixed final
    : public dataflow::TypedWorkerFacade<MixedConfig,
          dataflow::InputPorts<std::int32_t, float>,
          dataflow::OutputPorts<float, std::uint8_t>> {
public:
    static void validate(const MixedConfig& config);
    static dataflow::WorkerPorts ports(const MixedConfig& config);

    explicit Mixed(const MixedConfig&) {}

private:
    void work_one(input_views& inputs, output_views& outputs) override {
        const auto& integers = std::get<0>(inputs);
        const auto& fractions = std::get<1>(inputs);
        auto& sums = std::get<0>(outputs);
        auto& flags = std::get<1>(outputs);
        // Process one transaction.
    }
};
```

The graph still passes type-erased queue vectors at its boundary. `TypedWorker`
checks their count and scalar types once during binding, stores typed queue
tuples, and creates transaction/view tuples without framework allocation.
Empty packs support sources and sinks. Configuration-dependent port counts,
such as broadcaster outputs, should continue to use a homogeneous or custom
worker implementation. The lower-level `TypedWorker` remains available when a
worker needs direct control over construction, binding, or readiness.

### Random streams

Stochastic workers can override `initialize_random_streams` to seed their
generator. The engine calls it once per worker instance before binding queues.

```cpp
void initialize_random_streams(const dataflow::WorkerContext& context) override {
    generator_ = context.make_random_engine(base_seed_);
}
```

`make_random_engine()` returns a seeded `std::mt19937_64`. It combines the
worker's base seed with the zero-based sweep point, replica index, and node
position in the scenario. Ordinary runs use point zero. Node positions give
different nodes separate streams even when they share a base seed.
`WorkerTestHarnessOptions::context` can supply these coordinates for worker tests.

## Testing a worker

Include `<dataflow/worker_test.h>` to exercise one worker without manually
creating or binding queues. The harness validates the configuration, constructs
queues from the worker's declared layout, and addresses ports by name:

```cpp
dataflow::WorkerTestHarness<Gain> test(
    GainConfig{.transaction_size = 3, .gain = 2.0F});

test.write_input<float>("input", {1.0F, 2.0F, 3.0F});
assert(test.process_one());

const auto output = test.read_output<float>("output");
assert((output == std::vector<float>{2.0F, 4.0F, 6.0F}));
```

By default each queue holds two transactions. Pass
`WorkerTestHarnessOptions{.queue_capacity_transactions = N}` when a test needs
more buffered transactions. `worker()` exposes the concrete worker for checking
algorithm state, while `is_ready()`, `process()`, `output_size()`, and the
counted `read_output()` overload cover common lifecycle assertions.

## Registering an observer

Include `<dataflow/observer.h>`. A configured observer derives from
`ConfiguredSizedTypedObserver<Derived, Config, Input, Output>` or
`ConfiguredTypedObserver<Derived, Config, InputPorts<...>, OutputPorts<...>>`.
Pass only its configuration to the base constructor. The graph supplies the
immutable port layout during binding, just as it does for worker facades. The
base retains the configuration and automatically creates empty aggregation
instances. The observer keeps its accumulator and supplies:

- `statistics()` for display and CSV output;
- `criterion_satisfied()` and `criterion_description()` for stopping;
- `merge_from_same()` for type-safe cross-replica aggregation.

For example, its constructor only needs to forward the standard arguments:

```cpp
using Base = dataflow::ConfiguredSizedTypedObserver<
    MyObserver, MyObserverConfig, float, float>;

explicit MyObserver(const MyObserverConfig& config) : Base(config) {}
```

The original `SizedTypedObserver` and `TypedObserver` bases remain available
when an observer needs custom empty-instance construction; those observers
provide `make_empty_same()` themselves.

Expose the observer configuration as `Config` just like an ordinary worker,
then register it explicitly as an observer:

```cpp
workers.register_observer<BerObserver<std::uint8_t>>("ber_u8");
```

The observer bases provide the type-erased `merge_from()` and `make_empty()`
operations. `validate` remains optional for observers as well. Configured bases
also expose protected `observer_config()` and `observer_ports()` accessors.
`observer_ports()` is available after the execution instance is bound;
aggregation and reporting methods should use the retained configuration because
empty aggregation instances are intentionally unbound.

Keep observer accumulators independent and additive. See
`include/dataflow/workers/ber_observer.h` for a complete
two-input observer with error-count and Wilson confidence-interval criteria.

Periodic snapshots use `runtime.statistics.publish_interval_seconds`,
independently of terminal reporting. A zero interval publishes after every
sweep; a larger value reduces aggregation overhead but can overshoot a
statistical stopping threshold.
The final CSV is always rebuilt from snapshots published after all replica
threads have stopped.
