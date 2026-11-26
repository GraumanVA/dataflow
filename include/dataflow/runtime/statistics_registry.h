#pragma once

#include <dataflow/graphs/config_graph.h>
#include <dataflow/graphs/execution_graph.h>
#include <dataflow/interfaces/statistics.h>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace dataflow {

struct PublishedStatistic {
    std::string observer_id;
    std::shared_ptr<const IStatisticsProvider> provider;
};

struct GraphStatisticsSnapshot {
    std::size_t replica{};
    std::size_t sweeps{};
    bool finished = false;
    std::exception_ptr error;
    std::vector<PublishedStatistic> observers;
};

class StatisticsRegistry {
    mutable std::mutex mutex_;
    mutable std::condition_variable changed_;
    std::vector<std::optional<GraphStatisticsSnapshot>> latest_;
    std::uint64_t generation_{};

public:
    explicit StatisticsRegistry(std::size_t replica_count);

    void publish(GraphStatisticsSnapshot snapshot);

    [[nodiscard]] std::vector<GraphStatisticsSnapshot> collect(
        std::uint64_t &generation) const;

    void wait_for_change(std::uint64_t generation) const;
};

[[nodiscard]] GraphStatisticsSnapshot collect_graph_statistics(
    std::size_t replica,
    std::size_t sweeps,
    bool finished,
    std::exception_ptr error,
    const ConfigGraph &config,
    const ExecutionGraph &graph);

} // namespace dataflow
