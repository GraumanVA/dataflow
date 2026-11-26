#pragma once

#include <dataflow/graphs/execution_graph.h>

#include <cstddef>
#include <functional>
#include <stop_token>

namespace dataflow {

enum class ReplicaExit {
    quiescent,
    callback_requested,
    stop_requested
};

struct ReplicaResult {
    ReplicaExit exit;
    std::size_t sweeps;
};

using SweepCallback = std::function<bool(std::size_t sweeps, bool progress)>;

class ReplicaExecutor {
public:
    [[nodiscard]] ReplicaResult run(ExecutionGraph &graph,
        std::stop_token stop,
        const SweepCallback &after_sweep = {}) const;
};

} // namespace dataflow
