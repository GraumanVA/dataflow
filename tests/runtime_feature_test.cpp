#include <dataflow/worker_registry.h>
#include <dataflow/worker_test.h>
#include <graphs/execution_graph.h>
#include <runtime/coordinator.h>
#include <workers/ber_observer.h>
#include <workers/broadcaster.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void require(bool condition, const char *message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

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

void test_mixed_type_worker() {
    dataflow::WorkerRegistry workers;
    workers.register_worker<MixedTypeWorker>("mixed");
    const auto prepared =
        workers.prepare("mixed", YAML::Load("transaction_size: 2"));

    require(prepared.ports->inputs.size() == 2,
        "mixed worker input layout was not prepared");
    require(prepared.ports->outputs.size() == 2,
        "mixed worker output layout was not prepared");
    require(prepared.ports->inputs[0].type == dataflow::ScalarType::i32 &&
                prepared.ports->inputs[1].type == dataflow::ScalarType::f32 &&
                prepared.ports->outputs[0].type == dataflow::ScalarType::f32 &&
                prepared.ports->outputs[1].type == dataflow::ScalarType::u8,
        "mixed worker port types were not preserved");

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
    require(worker->process_one(), "mixed worker made no progress");

    auto sum_read = sums->begin_read_exact(2);
    auto positive_read = positive->begin_read_exact(2);
    require(sum_read.spans()[0] == 1.5F && sum_read.spans()[1] == -1.75F,
        "mixed worker produced incorrect float output");
    require(positive_read.spans()[0] == 1 && positive_read.spans()[1] == 0,
        "mixed worker produced incorrect integer output");
}

void test_broadcaster_delivery() {
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
    require(broadcaster.process_one(), "broadcaster made no progress");
    require(input->empty(), "broadcaster did not consume its input");
    require(reliable->elems_stored() == 2,
        "reliable broadcaster output was not written");

    require(best_effort->elems_stored() == 2,
        "best-effort output should have skipped a full queue");
}

void test_stop_policies() {
    dataflow::StopController any(2, dataflow::StopPolicy::any);
    any.update(0, true);
    require(any.stop_requested(), "any stop policy did not stop");

    dataflow::StopController all(2, dataflow::StopPolicy::all);
    all.update(0, true);
    require(!all.stop_requested(), "all stop policy stopped too early");
    all.update(1, true);
    require(all.stop_requested(), "all stop policy did not stop");
}

void test_confidence_criterion() {
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
    require(observer.process_one(), "BER observer made no progress");
    require(observer.criterion_satisfied(),
        "BER confidence criterion was not satisfied");

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
    require(second.process_one(), "second BER observer made no progress");
    observer.merge_from(second);
    require(observer.statistics()[0].value == "16000",
        "BER observer merge did not add accumulator totals");

    const dataflow::BerObserverConfig unpacked_config{
        .transaction_size = 4,
        .bits_per_element = 1,
        .stop_mode = "bit_errors",
        .target_bit_errors = 2};
    dataflow::WorkerTestHarness<dataflow::BerObserver<std::int8_t>> unpacked(
        unpacked_config);
    unpacked.write_input<std::int8_t>("reference", {0, 1, 0, 1});
    unpacked.write_input<std::int8_t>("received", {1, 1, 1, 1});
    require(unpacked.process_one(), "unpacked BER observer made no progress");
    const auto unpacked_statistics = unpacked.worker().statistics();
    require(unpacked_statistics[0].value == "4" &&
                unpacked_statistics[1].value == "2" &&
                unpacked_statistics[2].value == "5.000000e-01" &&
                unpacked_statistics.size() == 3,
        "unpacked BER observer did not count one logical bit per element");
}

void test_coordinator_results() {
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

    require(first.statistics_file.filename() == "stats.csv",
        "coordinator did not choose stats.csv initially");
    require(second.statistics_file.filename() == "stats_2.csv",
        "coordinator did not increment an existing report name");
    if (FiniteSource::validation_calls() != 4) {
        throw std::runtime_error(
            "worker configuration validation count was " +
            std::to_string(FiniteSource::validation_calls()) +
            ", expected 4 (once per configured node per run)");
    }
    require(FiniteSource::construction_calls() == 8,
        "coordinator did not construct every prepared worker replica");
    std::ifstream report(first.statistics_file);
    const std::string contents((std::istreambuf_iterator<char>(report)), {});
    require(contents.find("ber,bits,640") != std::string::npos,
        "coordinator did not merge replica BER totals");
    report.close();

    std::filesystem::remove(first.statistics_file);
    std::filesystem::remove(second.statistics_file);
    std::filesystem::remove(statistics_root / "merge_case");
    std::filesystem::remove(statistics_root);
    std::filesystem::remove(config_path);
}

void test_sweep_results() {
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

    require(sweep.points.size() == 2,
        "coordinator produced the wrong number of sweep points");
    require(sweep.points[0].parameters[0].name == "offset" &&
                sweep.points[0].parameters[0].value == "0" &&
                sweep.points[0].parameters[0].display_name == "off" &&
                sweep.points[0].parameters[1].name == "case_id" &&
                sweep.points[0].parameters[1].value == "100" &&
                sweep.points[0].parameters[1].display_name == "id",
        "sweep point parameters were not reported");
    require(sweep.points[0].replicas.size() == 2 &&
                sweep.points[0].replicas[0].parameters[0].value == "0" &&
                sweep.points[0].replicas[1].parameters[0].value == "1",
        "replica-derived parameters were not reported");
    require(sweep.points[0].run.statistics_file.parent_path().filename() ==
                    "case_0000__off=0__id=100" &&
                sweep.points[1].run.statistics_file.parent_path().filename() ==
                    "case_0001__off=1__id=110",
        "sweep statistics were not separated by point");
    require(terminal.str().find("[Sweep case 0: off=0, id=100]") !=
                    std::string::npos &&
                terminal.str().find("[Sweep case 1: off=1, id=110]") !=
                    std::string::npos &&
                terminal.str().find("\x1b[H\x1b[J") == std::string::npos,
        "sweep terminal tables lost their context or cleared prior cases");

    std::ifstream first_report(sweep.points[0].run.statistics_file);
    std::ifstream second_report(sweep.points[1].run.statistics_file);
    const std::string first_contents(
        (std::istreambuf_iterator<char>(first_report)),
        {});
    const std::string second_contents(
        (std::istreambuf_iterator<char>(second_report)),
        {});
    require(first_contents.find("ber,bits,64") != std::string::npos &&
                first_contents.find("ber,bit_errors,1") != std::string::npos &&
                first_contents.find("ci_lower") == std::string::npos,
        "first sweep point did not merge replica statistics");
    require(second_contents.find("ber,bits,64") != std::string::npos &&
                second_contents.find("ber,bit_errors,2") != std::string::npos &&
                second_contents.find("ci_lower") == std::string::npos,
        "second sweep point statistics were not independent");

    first_report.close();
    second_report.close();
    std::filesystem::remove_all(statistics_root);
    std::filesystem::remove(config_path);
}

} // namespace

int main() {
    try {
        test_stop_policies();
        test_mixed_type_worker();
        test_broadcaster_delivery();
        test_confidence_criterion();
        test_coordinator_results();
        test_sweep_results();
    } catch (const std::exception &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
