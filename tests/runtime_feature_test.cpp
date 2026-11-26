#include <dataflow/worker_registry.h>
#include <dataflow/worker_test.h>
#include <dataflow/graphs/execution_graph.h>
#include <dataflow/runtime/coordinator.h>
#include <dataflow/workers/ber_observer.h>
#include <dataflow/workers/broadcaster.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

struct SourceConfig {
    std::size_t transaction_size = 1;
    std::size_t transactions = 1;
    std::int32_t value = 0;
    std::int32_t seed = 0;
    std::int32_t case_tag = 0;
};

class FiniteSource final
    : public dataflow::SizedTypedWorker<std::int32_t, std::int32_t> {
    SourceConfig config_;
    std::size_t emitted_{};
    inline static std::size_t validation_calls_{};
    inline static std::size_t construction_calls_{};

public:
    using Config = SourceConfig;
    static void validate(const SourceConfig &config) {
        ++validation_calls_;
        if (config.transaction_size == 0) {
            throw std::invalid_argument(
                "source transaction size must be positive");
        }
    }

    [[nodiscard]] static dataflow::WorkerPorts ports(
        const SourceConfig &config) {
        return {{},
            {{"output", dataflow::ScalarType::i32, config.transaction_size}}};
    }

    FiniteSource(const SourceConfig &config,
        std::shared_ptr<const dataflow::WorkerPorts> ports)
        : SizedTypedWorker(std::move(ports)), config_(config) {
        ++construction_calls_;
    }

    static void reset_lifecycle_counts() noexcept {
        validation_calls_ = 0;
        construction_calls_ = 0;
    }

    [[nodiscard]] static std::size_t validation_calls() noexcept {
        return validation_calls_;
    }

    [[nodiscard]] static std::size_t construction_calls() noexcept {
        return construction_calls_;
    }

    [[nodiscard]] bool is_ready() const override {
        return emitted_ < config_.transactions && SizedTypedWorker::is_ready();
    }

    void work_one(
        std::span<dataflow::CyclicQueue<std::int32_t>::const_read_view>,
        std::span<dataflow::CyclicQueue<std::int32_t>::read_view> outputs)
        override {
        for (auto &value : outputs[0].first) {
            value = config_.value + config_.seed;
        }
        for (auto &value : outputs[0].second) {
            value = config_.value + config_.seed;
        }
        ++emitted_;
    }
};

struct MixedTypeConfig {
    std::size_t transaction_size = 1;
};

class MixedTypeWorker final
    : public dataflow::TypedWorker<dataflow::InputPorts<std::int32_t, float>,
          dataflow::OutputPorts<float, std::uint8_t>> {
    using Base =
        dataflow::TypedWorker<dataflow::InputPorts<std::int32_t, float>,
            dataflow::OutputPorts<float, std::uint8_t>>;

public:
    using Config = MixedTypeConfig;
    static void validate(const MixedTypeConfig &config) {
        if (config.transaction_size == 0) {
            throw std::invalid_argument(
                "mixed worker transaction size must be positive");
        }
    }

    [[nodiscard]] static dataflow::WorkerPorts ports(
        const MixedTypeConfig &config) {
        return {
            {{"integers", dataflow::ScalarType::i32, config.transaction_size},
                {"fractions",
                    dataflow::ScalarType::f32,
                    config.transaction_size}},
            {{"sums", dataflow::ScalarType::f32, config.transaction_size},
                {"positive",
                    dataflow::ScalarType::u8,
                    config.transaction_size}}};
    }

    MixedTypeWorker(const MixedTypeConfig &,
        std::shared_ptr<const dataflow::WorkerPorts> ports)
        : Base(std::move(ports)) {
    }

private:
    void work_one(input_views &inputs, output_views &outputs) override {
        const auto &integers = std::get<0>(inputs);
        const auto &fractions = std::get<1>(inputs);
        auto &sums = std::get<0>(outputs);
        auto &positive = std::get<1>(outputs);

        for (std::size_t i{}; i < integers.size(); ++i) {
            sums[i] = static_cast<float>(integers[i]) + fractions[i];
            positive[i] = integers[i] > 0 ? std::uint8_t{1} : std::uint8_t{0};
        }
    }
};

TEST(RuntimeFeatures, ExecutesWorkersWithMixedPortTypes) {
    dataflow::WorkerRegistry workers;
    workers.register_worker<MixedTypeWorker>("mixed");
    const auto prepared =
        workers.prepare("mixed", YAML::Load("transaction_size: 2"));

    ASSERT_EQ(prepared.ports->inputs.size(), 2u);
    ASSERT_EQ(prepared.ports->outputs.size(), 2u);
    EXPECT_EQ(prepared.ports->inputs[0].type, dataflow::ScalarType::i32);
    EXPECT_EQ(prepared.ports->inputs[1].type, dataflow::ScalarType::f32);
    EXPECT_EQ(prepared.ports->outputs[0].type, dataflow::ScalarType::f32);
    EXPECT_EQ(prepared.ports->outputs[1].type, dataflow::ScalarType::u8);

    auto integers = std::make_shared<dataflow::CyclicQueue<std::int32_t>>(2, 0);
    auto fractions = std::make_shared<dataflow::CyclicQueue<float>>(2, 1);
    auto sums = std::make_shared<dataflow::CyclicQueue<float>>(2, 2);
    auto positive = std::make_shared<dataflow::CyclicQueue<std::uint8_t>>(2, 3);

    {
        auto integer_write = integers->begin_write_exact(2);
        auto fraction_write = fractions->begin_write_exact(2);
        integer_write.spans()[0] = 1;
        integer_write.spans()[1] = -2;
        fraction_write.spans()[0] = 0.5F;
        fraction_write.spans()[1] = 0.25F;
        integer_write.commit_all();
        fraction_write.commit_all();
    }

    auto worker = prepared.create();
    worker->bind({{integers, fractions}, {sums, positive}});
    ASSERT_TRUE(worker->process_one());

    auto sum_read = sums->begin_read_exact(2);
    auto positive_read = positive->begin_read_exact(2);
    EXPECT_FLOAT_EQ(sum_read.spans()[0], 1.5F);
    EXPECT_FLOAT_EQ(sum_read.spans()[1], -1.75F);
    EXPECT_EQ(positive_read.spans()[0], 1);
    EXPECT_EQ(positive_read.spans()[1], 0);
}

TEST(RuntimeFeatures, BroadcasterSkipsFullBestEffortQueue) {
    auto input = std::make_shared<dataflow::CyclicQueue<std::uint8_t>>(4, 0);
    auto reliable = std::make_shared<dataflow::CyclicQueue<std::uint8_t>>(4, 1);
    auto best_effort =
        std::make_shared<dataflow::CyclicQueue<std::uint8_t>>(2, 2);

    {
        auto write = input->begin_write_exact(2);
        write.spans()[0] = 0x12;
        write.spans()[1] = 0x34;
        write.commit_all();
    }
    {
        auto write = best_effort->begin_write_exact(2);
        write.spans()[0] = 0;
        write.spans()[1] = 0;
        write.commit_all();
    }

    const dataflow::BroadcasterConfig broadcaster_config{2,
        {"reliable", "best_effort"}};
    dataflow::Broadcaster<std::uint8_t>::validate(broadcaster_config);
    auto broadcaster_ports = std::make_shared<const dataflow::WorkerPorts>(
        dataflow::Broadcaster<std::uint8_t>::ports(broadcaster_config));
    dataflow::Broadcaster<std::uint8_t> broadcaster(broadcaster_config,
        broadcaster_ports);
    broadcaster.bind({{input}, {reliable, best_effort}});
    ASSERT_TRUE(broadcaster.process_one());
    EXPECT_TRUE(input->empty());
    EXPECT_EQ(reliable->elems_stored(), 2u);
    EXPECT_EQ(best_effort->elems_stored(), 2u);
}

TEST(RuntimeFeatures, StopPoliciesApplyAnyAndAllCriteria) {
    dataflow::StopController any(2, dataflow::StopPolicy::any);
    any.update(0, true);
    EXPECT_TRUE(any.stop_requested());

    dataflow::StopController all(2, dataflow::StopPolicy::all);
    all.update(0, true);
    EXPECT_FALSE(all.stop_requested());
    all.update(1, true);
    EXPECT_TRUE(all.stop_requested());
}

TEST(BerObserver, SatisfiesConfidenceCriterionAndMergesTotals) {
    constexpr std::size_t elements = 1000;
    auto reference =
        std::make_shared<dataflow::CyclicQueue<std::uint8_t>>(elements, 0);
    auto received =
        std::make_shared<dataflow::CyclicQueue<std::uint8_t>>(elements, 1);
    {
        auto left = reference->begin_write_exact(elements);
        auto right = received->begin_write_exact(elements);
        for (std::size_t i{}; i < elements; ++i) {
            left.spans()[i] = 0;
            right.spans()[i] = 0;
        }
        left.commit_all();
        right.commit_all();
    }

    const dataflow::BerObserverConfig observer_config{
        .transaction_size = elements,
        .stop_mode = "confidence_interval",
        .target_bit_errors = 100,
        .confidence = 0.95,
        .max_half_width = 0.001,
        .min_bits = elements * 8};
    dataflow::BerObserver<std::uint8_t>::validate(observer_config);
    auto observer_ports = std::make_shared<const dataflow::WorkerPorts>(
        dataflow::BerObserver<std::uint8_t>::ports(observer_config));
    dataflow::BerObserver<std::uint8_t> observer(observer_config);
    observer.bind({{reference, received}, {}, observer_ports});
    ASSERT_TRUE(observer.process_one());
    EXPECT_TRUE(observer.criterion_satisfied());

    auto second_reference =
        std::make_shared<dataflow::CyclicQueue<std::uint8_t>>(elements, 2);
    auto second_received =
        std::make_shared<dataflow::CyclicQueue<std::uint8_t>>(elements, 3);
    {
        auto left = second_reference->begin_write_exact(elements);
        auto right = second_received->begin_write_exact(elements);
        for (std::size_t i{}; i < elements; ++i) {
            left.spans()[i] = 0;
            right.spans()[i] = 0;
        }
        left.commit_all();
        right.commit_all();
    }
    dataflow::BerObserver<std::uint8_t> second(observer_config);
    second.bind({{second_reference, second_received}, {}, observer_ports});
    ASSERT_TRUE(second.process_one());
    observer.merge_from(second);
    const auto statistics = observer.statistics();
    ASSERT_FALSE(statistics.empty());
    EXPECT_EQ(statistics[0].value, "16000");
}

TEST(BerObserver, CountsOneLogicalBitPerUnpackedElement) {
    const dataflow::BerObserverConfig unpacked_config{
        .transaction_size = 4,
        .bits_per_element = 1,
        .stop_mode = "bit_errors",
        .target_bit_errors = 2};
    dataflow::WorkerTestHarness<dataflow::BerObserver<std::int8_t>> unpacked(
        unpacked_config);
    unpacked.write_input<std::int8_t>("reference", {0, 1, 0, 1});
    unpacked.write_input<std::int8_t>("received", {1, 1, 1, 1});
    ASSERT_TRUE(unpacked.process_one());
    const auto unpacked_statistics = unpacked.worker().statistics();
    ASSERT_EQ(unpacked_statistics.size(), 3u);
    EXPECT_EQ(unpacked_statistics[0].value, "4");
    EXPECT_EQ(unpacked_statistics[1].value, "2");
    EXPECT_EQ(unpacked_statistics[2].value, "5.000000e-01");
}

TEST(Coordinator, MergesReplicaTotalsAndPreservesExistingReports) {
    const std::filesystem::path config_path = "coordinator_test.yaml";
    const std::filesystem::path statistics_root = "coordinator_statistics_test";
    std::filesystem::remove_all(statistics_root);
    {
        std::ofstream config(config_path);
        config << R"(scenario: merge_case
runtime:
  threads: 2
  stop:
    policy: all
    observers: [ber]
  reporting:
    terminal:
      enabled: true
nodes:
  - id: reference
    type: source
    params:
      transaction_size: 1
      transactions: 10
      value: 0
  - id: received
    type: source
    params:
      transaction_size: 1
      transactions: 10
      value: 1
  - id: ber
    type: ber_i32
    params:
      transaction_size: 1
      stop_mode: bit_errors
      target_bit_errors: 1000
edges:
  - producer: {node: reference, port: 0}
    consumer: {node: ber, port: 0}
    type: i32
  - producer: {node: received, port: 0}
    consumer: {node: ber, port: 1}
    type: i32
)";
    }

    dataflow::WorkerRegistry workers;
    workers.register_worker<FiniteSource>("source");
    workers.register_observer<dataflow::BerObserver<std::int32_t>>("ber_i32");

    const dataflow::CyclicQueueFactory queues;
    std::ostringstream terminal;
    const dataflow::Coordinator coordinator(workers,
        queues,
        statistics_root,
        terminal);
    FiniteSource::reset_lifecycle_counts();
    const auto first = coordinator.run(config_path);
    const auto second = coordinator.run(config_path);

    EXPECT_EQ(first.statistics_file.filename(), "stats.csv");
    EXPECT_EQ(second.statistics_file.filename(), "stats_2.csv");
    EXPECT_EQ(FiniteSource::validation_calls(), 4u);
    EXPECT_EQ(FiniteSource::construction_calls(), 8u);
    std::ifstream report(first.statistics_file);
    const std::string contents((std::istreambuf_iterator<char>(report)), {});
    EXPECT_NE(contents.find("ber,bits,640"), std::string::npos);
    report.close();

    std::filesystem::remove(first.statistics_file);
    std::filesystem::remove(second.statistics_file);
    std::filesystem::remove(statistics_root / "merge_case");
    std::filesystem::remove(statistics_root);
    std::filesystem::remove(config_path);
}

TEST(Coordinator, ReportsSweepPointsAndReplicaParameters) {
    const std::filesystem::path config_path = "sweep_test.yaml";
    const std::filesystem::path statistics_root = "sweep_statistics_test";
    std::filesystem::remove_all(statistics_root);
    {
        std::ofstream config(config_path);
        config << R"(scenario: sweep_case
runtime:
  threads: 2
  statistics:
    publish_interval_seconds: 0
  stop:
    observers: [ber]
  reporting:
    terminal:
      enabled: true
      interval_seconds: 0
      update_in_place: true
nodes:
  - id: reference
    type: source
    params:
      transactions: 1
  - id: received
    type: source
    params:
      transactions: 1
  - id: ber
    type: ber_i32
    params:
      stop_mode: bit_errors
      target_bit_errors: 1000
edges:
  - producer: {node: reference, port: output}
    consumer: {node: ber, port: reference}
  - producer: {node: received, port: output}
    consumer: {node: ber, port: received}
sweep:
  case_naming: parameters
  axes:
    - name: offset
      display_name: off
      values: [0, 1]
      targets:
        - {node: received, param: value}
  derived:
    - name: case_id
      display_name: id
      values:
        serial: {start: 100, step: 10}
      targets:
        - {node: received, param: case_tag}
  replica_parameters:
    - name: seed
      values:
        serial: {start: 0, step: 1}
      targets:
        - {node: received, param: seed}
)";
    }

    dataflow::WorkerRegistry workers;
    workers.register_worker<FiniteSource>("source");
    workers.register_observer<dataflow::BerObserver<std::int32_t>>("ber_i32");
    const dataflow::CyclicQueueFactory queues;
    std::ostringstream terminal;
    const dataflow::Coordinator coordinator(workers,
        queues,
        statistics_root,
        terminal);
    const auto sweep = coordinator.run_sweep(config_path);

    ASSERT_EQ(sweep.points.size(), 2u);
    const auto &first = sweep.points[0];
    const auto &second = sweep.points[1];
    ASSERT_EQ(first.parameters.size(), 2u);
    EXPECT_EQ(first.parameters[0].name, "offset");
    EXPECT_EQ(first.parameters[0].value, "0");
    EXPECT_EQ(first.parameters[0].display_name, "off");
    EXPECT_EQ(first.parameters[1].name, "case_id");
    EXPECT_EQ(first.parameters[1].value, "100");
    EXPECT_EQ(first.parameters[1].display_name, "id");
    ASSERT_EQ(first.replicas.size(), 2u);
    ASSERT_EQ(first.replicas[0].parameters.size(), 1u);
    ASSERT_EQ(first.replicas[1].parameters.size(), 1u);
    EXPECT_EQ(first.replicas[0].parameters[0].value, "0");
    EXPECT_EQ(first.replicas[1].parameters[0].value, "1");
    EXPECT_EQ(first.run.statistics_file.parent_path().filename(),
        "case_0000__off=0__id=100");
    EXPECT_EQ(second.run.statistics_file.parent_path().filename(),
        "case_0001__off=1__id=110");
    const auto output = terminal.str();
    EXPECT_NE(output.find("[Sweep case 0: off=0, id=100]"), std::string::npos);
    EXPECT_NE(output.find("[Sweep case 1: off=1, id=110]"), std::string::npos);
    EXPECT_EQ(output.find("\x1b[H\x1b[J"), std::string::npos);

    std::ifstream first_report(first.run.statistics_file);
    std::ifstream second_report(second.run.statistics_file);
    const std::string first_contents(
        (std::istreambuf_iterator<char>(first_report)),
        {});
    const std::string second_contents(
        (std::istreambuf_iterator<char>(second_report)),
        {});
    EXPECT_NE(first_contents.find("ber,bits,64"), std::string::npos);
    EXPECT_NE(first_contents.find("ber,bit_errors,1"), std::string::npos);
    EXPECT_EQ(first_contents.find("ci_lower"), std::string::npos);
    EXPECT_NE(second_contents.find("ber,bits,64"), std::string::npos);
    EXPECT_NE(second_contents.find("ber,bit_errors,2"), std::string::npos);
    EXPECT_EQ(second_contents.find("ci_lower"), std::string::npos);

    first_report.close();
    second_report.close();
    std::filesystem::remove_all(statistics_root);
    std::filesystem::remove(config_path);
}

} // namespace
