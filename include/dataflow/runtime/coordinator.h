#pragma once

#include <dataflow/run_result.h>
#include <dataflow/sweep_result.h>
#include <dataflow/graphs/config_graph.h>
#include <dataflow/interfaces/worker_factory.h>
#include <dataflow/storage/cyclic_queue.h>

#include <cstddef>
#include <filesystem>
#include <iosfwd>
#include <vector>

namespace dataflow {

class Coordinator {
    const IWorkerFactory &worker_factory_;
    const CyclicQueueFactory &queue_factory_;
    std::filesystem::path statistics_root_;
    std::ostream &terminal_;

    [[nodiscard]] RunResult run_replicas(const ConfigGraph &config,
        const std::vector<ConfigGraph> &replica_configs,
        const std::filesystem::path &statistics_directory,
        std::string report_context = {},
        std::size_t sweep_point = 0) const;

public:
    Coordinator(const IWorkerFactory &worker_factory,
        const CyclicQueueFactory &queue_factory,
        std::filesystem::path statistics_root,
        std::ostream &terminal);

    [[nodiscard]] RunResult run(const std::filesystem::path &config_file) const;
    [[nodiscard]] RunResult run(const ConfigGraph &config) const;
    [[nodiscard]] SweepResult run_sweep(
        const std::filesystem::path &config_file) const;
    [[nodiscard]] SweepResult run_sweep(const ConfigGraph &config) const;
};

} // namespace dataflow
