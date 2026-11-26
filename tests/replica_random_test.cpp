#include <dataflow/engine.h>

#include "../examples/bsc_channel/bsc_workers.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

namespace {

struct CaptureConfig {
    std::size_t batch_size = 64;
};

class Capture final : public dataflow::ConfiguredTypedObserver<Capture,
                          CaptureConfig,
                          dataflow::InputPorts<std::int8_t, std::int8_t>,
                          dataflow::OutputPorts<>> {
    std::vector<dataflow::StatisticValue> samples_;

public:
    using Config = CaptureConfig;

    explicit Capture(const Config &config) : ConfiguredTypedObserver(config) {}

    static dataflow::WorkerPorts ports(const Config &config) {
        return {{dataflow::port<std::int8_t>("reference", config.batch_size),
                    dataflow::port<std::int8_t>("received", config.batch_size)},
            {}};
    }

    [[nodiscard]] bool is_ready() const override {
        return samples_.empty() && ConfiguredTypedObserver::is_ready();
    }

    [[nodiscard]] std::vector<dataflow::StatisticValue> statistics()
        const override {
        return samples_;
    }

    void merge_from_same(const Capture &other) {
        samples_.insert(samples_.end(), other.samples_.begin(),
            other.samples_.end());
    }

private:
    void work_one(input_views &inputs, output_views &) override {
        const auto &[reference, received] = inputs;
        std::string source;
        std::string flips;
        for (std::size_t index{}; index < reference.size(); ++index) {
            source += reference[index] == 0 ? '0' : '1';
            flips += reference[index] == received[index] ? '0' : '1';
        }
        samples_.push_back({"source", std::move(source)});
        samples_.push_back({"flips", std::move(flips)});
    }
};

void write_scenario(const std::filesystem::path &path,
    bool customize_replica_seed) {
    std::ofstream config(path);
    config << R"(scenario: random_streams
runtime:
  threads: 4
sweep:
  axes:
    - name: probability
      values: [0.5, 0.5]
      targets: [{node: channel, param: crossover_probability}]
)";
    if (customize_replica_seed) {
        config << R"(  replica_parameters:
    - name: channel_seed
      values: {serial: {start: 0, step: 1}}
      targets: [{node: channel, param: seed}]
)";
    }
    config << R"(nodes:
  - id: source
    type: random_bits_i8
    params: {emitted_block_size: 64}
  - id: fanout
    type: broadcast_i8
    params: {transaction_size: 64, delivery: [reliable, reliable]}
  - id: channel
    type: bsc_i8
    params: {accepted_size: 64}
  - id: capture
    type: capture
    params: {}
edges:
  - producer: {node: source, port: bits}
    consumer: {node: fanout, port: input}
  - producer: {node: fanout, port: output_0}
    consumer: {node: channel, port: input}
  - producer: {node: fanout, port: output_1}
    consumer: {node: capture, port: reference}
  - producer: {node: channel, port: output}
    consumer: {node: capture, port: received}
)";
}

std::vector<std::string> read_csv(const std::filesystem::path &path) {
    std::ifstream input(path);
    std::vector<std::string> rows;
    for (std::string row; std::getline(input, row);) {
        rows.push_back(std::move(row));
    }
    return rows;
}

class ReplicaRandomTest : public testing::TestWithParam<bool> {
protected:
    const std::filesystem::path root = std::filesystem::temp_directory_path() /
        ("dataflow-random-test-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));

    void SetUp() override {
        ASSERT_TRUE(std::filesystem::create_directory(root));
    }

    void TearDown() override {
        std::error_code ignored;
        std::filesystem::remove_all(root, ignored);
    }
};

TEST_P(ReplicaRandomTest, SeparatesBscStreamsAndRepeatsWithSameConfiguration) {
    const auto scenario = root / "scenario.yaml";
    write_scenario(scenario, GetParam());
    dataflow::Engine engine({.statistics_root = root / "statistics"});
    example::bsc::register_workers(engine.workers());
    engine.workers().register_observer<Capture>("capture");

    const auto first = engine.run_sweep(scenario);
    const auto second = engine.run_sweep(scenario);
    ASSERT_EQ(first.points.size(), 2u);
    ASSERT_EQ(second.points.size(), first.points.size());

    std::set<std::string> source_sequences;
    std::set<std::string> flip_sequences;
    for (std::size_t point{}; point < first.points.size(); ++point) {
        const auto &run = first.points[point].run;
        EXPECT_EQ(run.termination, dataflow::RunTermination::quiescent);
        const auto rows = read_csv(run.statistics_file);
        ASSERT_EQ(rows.size(), 9u);
        EXPECT_EQ(rows, read_csv(second.points[point].run.statistics_file));
        constexpr std::string_view source_prefix = "capture,source,";
        constexpr std::string_view flips_prefix = "capture,flips,";
        for (std::size_t row = 1; row < rows.size(); row += 2) {
            ASSERT_TRUE(rows[row].starts_with(source_prefix));
            ASSERT_TRUE(rows[row + 1].starts_with(flips_prefix));
            source_sequences.insert(rows[row].substr(source_prefix.size(), 64));
            flip_sequences.insert(
                rows[row + 1].substr(flips_prefix.size(), 64));
        }
    }
    EXPECT_EQ(source_sequences.size(), 8u);
    EXPECT_EQ(flip_sequences.size(), 8u);
    for (const auto &sequence : source_sequences) {
        EXPECT_FALSE(flip_sequences.contains(sequence));
    }
}

INSTANTIATE_TEST_SUITE_P(ReplicaConfigurations, ReplicaRandomTest,
    testing::Bool());

} // namespace
