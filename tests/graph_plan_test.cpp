#include <dataflow/worker_registry.h>
#include <graphs/execution_graph.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

struct TestWorkerConfig {
    std::vector<std::size_t> inputs;
    std::vector<std::size_t> outputs;
};

class TestWorker final
    : public dataflow::SizedTypedWorker<std::int32_t, std::int32_t> {
public:
    using Config = TestWorkerConfig;
    static void validate(const TestWorkerConfig &config) {
        const auto positive = [](std::size_t size) { return size > 0; };
        if (!std::ranges::all_of(config.inputs, positive) ||
            !std::ranges::all_of(config.outputs, positive)) {
            throw std::invalid_argument(
                "test worker transaction sizes must be positive");
        }
    }

    [[nodiscard]] static dataflow::WorkerPorts ports(
        const TestWorkerConfig &config) {
        dataflow::WorkerPorts result;
        for (std::size_t i{}; i < config.inputs.size(); ++i) {
            result.inputs.push_back({"input_" + std::to_string(i),
                dataflow::ScalarType::i32,
                config.inputs[i]});
        }
        for (std::size_t i{}; i < config.outputs.size(); ++i) {
            result.outputs.push_back({"output_" + std::to_string(i),
                dataflow::ScalarType::i32,
                config.outputs[i]});
        }
        return result;
    }

    TestWorker(const TestWorkerConfig &,
        std::shared_ptr<const dataflow::WorkerPorts> ports)
        : SizedTypedWorker(std::move(ports)) {
    }

    void work_one(
        std::span<dataflow::CyclicQueue<std::int32_t>::const_read_view>,
        std::span<dataflow::CyclicQueue<std::int32_t>::read_view>) override {
    }
};

void require(bool condition, const char *message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void require_unknown_field_rejected(std::string yaml,
    std::string_view expected_context) {
    try {
        std::istringstream input(std::move(yaml));
        static_cast<void>(dataflow::ConfigGraph(input));
        throw std::runtime_error("unknown graph field was accepted");
    } catch (const std::invalid_argument &error) {
        require(std::string_view(error.what()).find(expected_context) !=
                    std::string_view::npos,
            "unknown field error did not identify its context");
    }
}

void test_sweep_expansion() {
    std::istringstream product_yaml(R"(
nodes:
  - id: probe
    type: unused
    params: {a: 0, b: 0.0, metadata: {case_id: 0}, seed: 0}
edges: []
sweep:
  combine: product
  case_naming: parameters
  axes:
    - name: a
      display_name: A
      values: [10, 20]
      targets:
        - {node: probe, param: a}
    - name: b
      values:
        range: {start: -1.0, stop: 0.0, step: 0.5}
      targets:
        - {node: probe, path: [b]}
  derived:
    - name: case_id
      display_name: id
      values:
        serial: {start: 100, step: 10}
      targets:
        - {node: probe, path: [metadata, case_id]}
  replica_parameters:
    - name: seed
      values:
        serial: {start: 7, step: 2}
      targets:
        - {node: probe, param: seed}
)");
    const dataflow::ConfigGraph product(product_yaml);
    require(product.has_sweep(), "configured sweep was not retained");
    const auto points = product.sweep().expand_points();
    require(points.size() == 6, "product sweep has the wrong point count");
    const auto directory = product.sweep().case_directory_name(points[0]);
    require(points[0].parameters[0].display_name == "A" &&
                points[0].parameters[2].display_name == "id" &&
                directory == "case_0000__A=10__b=-1__id=100",
        "display names were not applied to the sweep case directory");

    const auto first = product.with_assignments(points[0].assignments);
    require(first.node(0).params["a"].as<int>() == 10 &&
                first.node(0).params["b"].as<double>() == -1.0 &&
                first.node(0).params["metadata"]["case_id"].as<int>() == 100,
        "first product sweep point was expanded incorrectly");
    const auto fifth = product.with_assignments(points[4].assignments);
    require(fifth.node(0).params["a"].as<int>() == 20 &&
                fifth.node(0).params["b"].as<double>() == -0.5 &&
                fifth.node(0).params["metadata"]["case_id"].as<int>() == 140,
        "product sweep ordering or point serial was incorrect");

    const auto replicas = product.sweep().expand_replicas(3);
    require(replicas.size() == 3,
        "replica serial expansion has the wrong size");
    const auto third_replica = fifth.with_assignments(replicas[2].assignments);
    require(third_replica.node(0).params["seed"].as<int>() == 11,
        "replica serial was expanded incorrectly");

    std::istringstream zip_yaml(R"(
nodes:
  - id: probe
    type: unused
    params: {a: 0, b: 0}
edges: []
sweep:
  combine: zip
  axes:
    - name: a
      values: [1, 2]
      targets:
        - {node: probe, param: a}
    - name: b
      values:
        range: {start: 4, step: 2, count: 2}
      targets:
        - {node: probe, param: b}
)");
    const dataflow::ConfigGraph zipped(zip_yaml);
    const auto zipped_points = zipped.sweep().expand_points();
    require(zipped_points.size() == 2, "zip sweep has the wrong point count");
    const auto second = zipped.with_assignments(zipped_points[1].assignments);
    require(second.node(0).params["a"].as<int>() == 2 &&
                second.node(0).params["b"].as<int>() == 6,
        "zip sweep paired values incorrectly");

    std::istringstream invalid_zip_yaml(R"(
nodes:
  - id: probe
    type: unused
    params: {a: 0, b: 0}
edges: []
sweep:
  combine: zip
  axes:
    - name: a
      values: [1, 2]
      targets: [{node: probe, param: a}]
    - name: b
      values: [3]
      targets: [{node: probe, param: b}]
)");
    try {
        const dataflow::ConfigGraph invalid_zip(invalid_zip_yaml);
        static_cast<void>(invalid_zip.sweep().expand_points());
        throw std::runtime_error("mismatched zipped axes were accepted");
    } catch (const std::invalid_argument &) {
    }
}

} // namespace

int main() {
    std::istringstream yaml(R"(
nodes:
  - id: source
    type: source
    params:
      inputs: []
      outputs: [2]
  - id: sink
    type: sink
    params:
      inputs: [3]
      outputs: []
edges:
  - producer: {node: source, port: output_0}
    consumer: {node: sink, port: input_0}
)");

    dataflow::WorkerRegistry workers;
    workers.register_worker<TestWorker>("source");
    workers.register_worker<TestWorker>("sink");

    const dataflow::ConfigGraph config(yaml);
    const auto plan = dataflow::GraphCompiler{}.compile(config, workers);

    require(plan.workers.size() == 2, "wrong worker count");
    require(plan.queues.size() == 1, "wrong queue count");
    require(plan.queues.front().capacity == 5,
        "default queue capacity was not calculated from port sizes");
    require(plan.topological_order.size() == 2, "wrong schedule size");

    const dataflow::CyclicQueueFactory queues;
    const dataflow::ExecutionGraph graph(config, plan, queues);
    require(graph.workers().size() == 2, "workers were not materialized");
    require(graph.queues().size() == 1, "queues were not materialized");
    require(graph.queues().front()->type() == dataflow::ScalarType::i32,
        "queue has the wrong scalar type");

    std::istringstream invalid_yaml(R"(
nodes:
  - id: source
    type: source
    params: {inputs: [], outputs: [1]}
  - id: sink
    type: sink
    params: {inputs: [1], outputs: []}
edges:
  - producer: {node: source, port: missing}
    consumer: {node: sink, port: input_0}
)");
    try {
        const dataflow::ConfigGraph invalid(invalid_yaml);
        static_cast<void>(dataflow::GraphCompiler{}.compile(invalid, workers));
        throw std::runtime_error("unknown named port was accepted");
    } catch (const dataflow::GraphCompilationError &error) {
        require(!error.diagnostics().empty(),
            "named-port error did not contain diagnostics");
    }

    require_unknown_field_rejected("nodes: []\nedges: []\nunknown: true\n",
        "graph");
    require_unknown_field_rejected(
        "runtime: {unknown: true}\nnodes: []\nedges: []\n",
        "runtime");
    require_unknown_field_rejected(
        "nodes: [{id: source, type: source, unknown: true}]\nedges: []\n",
        "worker");
    require_unknown_field_rejected(
        "nodes: []\nedges: [{producer: {node: a, port: out}, consumer: "
        "{node: b, port: in}, unknown: true}]\n",
        "edge");
    require_unknown_field_rejected(
        "nodes: []\nedges: [{producer: {node: a, port: out, unknown: true}, "
        "consumer: {node: b, port: in}}]\n",
        "producer");
    test_sweep_expansion();
}
