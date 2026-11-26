#include <dataflow/worker_registry.h>
#include <dataflow/graphs/execution_graph.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
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

void expect_unknown_field_rejected(std::string yaml,
    std::string_view expected_context) {
    SCOPED_TRACE(expected_context);
    try {
        std::istringstream input(std::move(yaml));
        static_cast<void>(dataflow::ConfigGraph(input));
        ADD_FAILURE() << "Unknown graph field was accepted";
    } catch (const std::invalid_argument &error) {
        EXPECT_NE(std::string_view(error.what()).find(expected_context),
            std::string_view::npos) << error.what();
    }
}

TEST(GraphSweep, ExpandsProductWithSerialParameters) {
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
    ASSERT_TRUE(product.has_sweep());
    const auto points = product.sweep().expand_points();
    ASSERT_EQ(points.size(), 6u);
    ASSERT_EQ(points[0].parameters.size(), 3u);
    const auto directory = product.sweep().case_directory_name(points[0]);
    EXPECT_EQ(points[0].parameters[0].display_name, "A");
    EXPECT_EQ(points[0].parameters[2].display_name, "id");
    EXPECT_EQ(directory, "case_0000__A=10__b=-1__id=100");

    const auto first = product.with_assignments(points[0].assignments);
    EXPECT_EQ(first.node(0).params["a"].as<int>(), 10);
    EXPECT_DOUBLE_EQ(first.node(0).params["b"].as<double>(), -1.0);
    EXPECT_EQ(first.node(0).params["metadata"]["case_id"].as<int>(), 100);
    const auto fifth = product.with_assignments(points[4].assignments);
    EXPECT_EQ(fifth.node(0).params["a"].as<int>(), 20);
    EXPECT_DOUBLE_EQ(fifth.node(0).params["b"].as<double>(), -0.5);
    EXPECT_EQ(fifth.node(0).params["metadata"]["case_id"].as<int>(), 140);

    const auto replicas = product.sweep().expand_replicas(3);
    ASSERT_EQ(replicas.size(), 3u);
    const auto third_replica = fifth.with_assignments(replicas[2].assignments);
    EXPECT_EQ(third_replica.node(0).params["seed"].as<int>(), 11);
}

TEST(GraphSweep, PairsZipAxesByIndex) {
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
    ASSERT_EQ(zipped_points.size(), 2u);
    const auto second = zipped.with_assignments(zipped_points[1].assignments);
    EXPECT_EQ(second.node(0).params["a"].as<int>(), 2);
    EXPECT_EQ(second.node(0).params["b"].as<int>(), 6);
}

TEST(GraphSweep, RejectsZipAxesOfDifferentLengths) {
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
    EXPECT_THROW({
        const dataflow::ConfigGraph invalid_zip(invalid_zip_yaml);
        static_cast<void>(invalid_zip.sweep().expand_points());
    }, std::invalid_argument);
}

TEST(GraphPlan, CompilesAndMaterializesNamedPorts) {
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

    EXPECT_EQ(plan.workers.size(), 2u);
    ASSERT_EQ(plan.queues.size(), 1u);
    EXPECT_EQ(plan.queues.front().capacity, 5u);
    EXPECT_EQ(plan.topological_order.size(), 2u);

    const dataflow::CyclicQueueFactory queues;
    const dataflow::ExecutionGraph graph(config, plan, queues);
    EXPECT_EQ(graph.workers().size(), 2u);
    ASSERT_EQ(graph.queues().size(), 1u);
    EXPECT_EQ(graph.queues().front()->type(), dataflow::ScalarType::i32);
}

TEST(GraphPlan, ReportsDiagnosticsForAnUnknownPort) {
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
    dataflow::WorkerRegistry workers;
    workers.register_worker<TestWorker>("source");
    workers.register_worker<TestWorker>("sink");

    try {
        const dataflow::ConfigGraph invalid(invalid_yaml);
        static_cast<void>(dataflow::GraphCompiler{}.compile(invalid, workers));
        ADD_FAILURE() << "Unknown named port was accepted";
    } catch (const dataflow::GraphCompilationError &error) {
        EXPECT_FALSE(error.diagnostics().empty());
    }
}

TEST(GraphPlan, ReportsContextForUnknownYamlFields) {
    expect_unknown_field_rejected("nodes: []\nedges: []\nunknown: true\n",
        "graph");
    expect_unknown_field_rejected(
        "runtime: {unknown: true}\nnodes: []\nedges: []\n",
        "runtime");
    expect_unknown_field_rejected(
        "nodes: [{id: source, type: source, unknown: true}]\nedges: []\n",
        "worker");
    expect_unknown_field_rejected(
        "nodes: []\nedges: [{producer: {node: a, port: out}, consumer: "
        "{node: b, port: in}, unknown: true}]\n",
        "edge");
    expect_unknown_field_rejected(
        "nodes: []\nedges: [{producer: {node: a, port: out, unknown: true}, "
        "consumer: {node: b, port: in}}]\n",
        "producer");
}

} // namespace
