#include <runtime/replica_executor.h>

namespace dataflow {

ReplicaResult ReplicaExecutor::run(ExecutionGraph &graph,
    std::stop_token stop,
    const SweepCallback &after_sweep) const {
    std::size_t sweeps{};
    while (!stop.stop_requested()) {
        bool progress = false;
        for (const auto worker : graph.schedule()) {
            progress = graph.workers()[worker]->process_one() || progress;
        }
        ++sweeps;

        if (after_sweep && after_sweep(sweeps, progress)) {
            return {ReplicaExit::callback_requested, sweeps};
        }
        if (!progress) {
            return {ReplicaExit::quiescent, sweeps};
        }
    }
    return {ReplicaExit::stop_requested, sweeps};
}

} // namespace dataflow
