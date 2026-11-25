#pragma once

#include <graphs/config_graph.h>
#include <graphs/execution_plan.h>
#include <interfaces/iworker.h>
#include <interfaces/worker_factory.h>
#include <storage/cyclic_queue.h>

#include <memory>
#include <vector>

namespace dataflow {

class ExecutionGraph {
    std::vector<std::shared_ptr<IQueue>> queues_;
    std::vector<std::unique_ptr<IWorker>> workers_;
    std::vector<worker_id> schedule_;

    void materialize(const ConfigGraph &config,
        const ExecutionPlan &plan,
        const CyclicQueueFactory &queue_factory);

public:
    ExecutionGraph(const ConfigGraph &config,
        const IWorkerFactory &worker_factory,
        const CyclicQueueFactory &queue_factory);

    ExecutionGraph(const ConfigGraph &config,
        const ExecutionPlan &plan,
        const CyclicQueueFactory &queue_factory);

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
