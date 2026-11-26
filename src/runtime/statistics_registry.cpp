#include <dataflow/runtime/statistics_registry.h>

#include <stdexcept>
#include <utility>

namespace dataflow {

StatisticsRegistry::StatisticsRegistry(std::size_t replica_count)
    : latest_(replica_count) {
    if (replica_count == 0) {
        throw std::invalid_argument(
            "statistics registry requires at least one replica");
    }
}

void StatisticsRegistry::publish(GraphStatisticsSnapshot snapshot) {
    {
        std::lock_guard lock(mutex_);
        if (snapshot.replica >= latest_.size()) {
            throw std::out_of_range("invalid statistics replica id");
        }
        latest_[snapshot.replica] = std::move(snapshot);
        ++generation_;
    }
    changed_.notify_one();
}

std::vector<GraphStatisticsSnapshot> StatisticsRegistry::collect(
    std::uint64_t &generation) const {
    std::lock_guard lock(mutex_);
    std::vector<GraphStatisticsSnapshot> result;
    result.reserve(latest_.size());
    for (const auto &snapshot : latest_) {
        if (snapshot) {
            result.push_back(*snapshot);
        }
    }
    generation = generation_;
    return result;
}

void StatisticsRegistry::wait_for_change(std::uint64_t generation) const {
    std::unique_lock lock(mutex_);
    changed_.wait(lock,
        [this, generation] { return generation_ != generation; });
}

GraphStatisticsSnapshot collect_graph_statistics(std::size_t replica,
    std::size_t sweeps,
    bool finished,
    std::exception_ptr error,
    const ConfigGraph &config,
    const ExecutionGraph &graph) {
    GraphStatisticsSnapshot snapshot{replica, sweeps, finished, error, {}};
    for (std::size_t worker{}; worker < graph.workers().size(); ++worker) {
        const auto *provider = dynamic_cast<const IStatisticsProvider *>(
            graph.workers()[worker].get());
        if (provider == nullptr) {
            continue;
        }

        auto frozen = provider->make_empty();
        frozen->merge_from(*provider);
        snapshot.observers.push_back({config.node(worker).id,
            std::shared_ptr<const IStatisticsProvider>(std::move(frozen))});
    }
    return snapshot;
}

} // namespace dataflow
