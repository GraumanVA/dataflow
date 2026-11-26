#pragma once

#include <dataflow/graphs/config_graph.h>
#include <dataflow/graphs/execution_plan.h>
#include <dataflow/interfaces/iworker.h>
#include <dataflow/interfaces/worker_factory.h>
#include <dataflow/storage/cyclic_queue.h>

#include <cstddef>
#include <memory>
#include <vector>

namespace dataflow {

class ExecutionGraph {
    std::vector<std::shared_ptr<IQueue>> queues_;
    std::vector<std::unique_ptr<IWorker>> workers_;
    std::vector<worker_id> schedule_;

    void materialize(const ConfigGraph &config,
        const ExecutionPlan &plan,
        const CyclicQueueFactory &queue_factory,
        std::size_t sweep_point,
        std::size_t replica);

public:
    ExecutionGraph(const ConfigGraph &config,
        const IWorkerFactory &worker_factory,
        const CyclicQueueFactory &queue_factory);

    ExecutionGraph(const ConfigGraph &config,
        const ExecutionPlan &plan,
        const CyclicQueueFactory &queue_factory,
        std::size_t sweep_point = 0,
        std::size_t replica = 0);

    [[nodiscard]] const std::vector<std::shared_ptr<IQueue>> &queues()
        const noexcept {
        return queues_;
    }

    [[nodiscard]] const std::vector<std::unique_ptr<IWorker>> &workers()
        const noexcept {
        return workers_;
    }

    [[nodiscard]] const std::vector<worker_id> &schedule() const noexcept {
        return schedule_;
    }
};

} // namespace dataflow
