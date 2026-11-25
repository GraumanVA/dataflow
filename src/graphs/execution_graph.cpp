#include <graphs/execution_graph.h>

#include <stdexcept>
#include <utility>

namespace dataflow {

ExecutionGraph::ExecutionGraph(const ConfigGraph &config,
    const IWorkerFactory &worker_factory,
    const CyclicQueueFactory &queue_factory) {
    const auto plan = GraphCompiler{}.compile(config, worker_factory);
    materialize(config, plan, queue_factory);
}

ExecutionGraph::ExecutionGraph(const ConfigGraph &config,
    const ExecutionPlan &plan,
    const CyclicQueueFactory &queue_factory) {
    materialize(config, plan, queue_factory);
}

void ExecutionGraph::materialize(const ConfigGraph &config,
    const ExecutionPlan &plan,
    const CyclicQueueFactory &queue_factory) {
    queues_.resize(plan.queues.size());
    for (const auto &queue : plan.queues) {
        if (queue.id >= queues_.size()) {
            throw std::invalid_argument(
                "execution plan has an invalid queue id");
        }

        CyclicQueueParams params;
        params.id_ = queue.id;
        params.capacity_ = queue.capacity;
        params.type_ = queue.type;

        auto instance = queue_factory.create(params);
        if (!instance) {
            throw std::invalid_argument(
                "no queue implementation for planned type");
        }
        queues_[queue.id] = std::shared_ptr<IQueue>(std::move(instance));
    }

    workers_.resize(plan.workers.size());
    for (const auto &worker : plan.workers) {
        if (worker.id >= workers_.size()) {
            throw std::invalid_argument(
                "execution plan has an invalid worker id");
        }

        static_cast<void>(config.node(worker.config_node));
        auto instance = worker.prepared.create();
        if (!instance) {
            throw std::runtime_error("worker factory returned a null worker");
        }
        workers_[worker.id] = std::move(instance);
    }

    for (const auto &worker : plan.workers) {
        WorkerBindings bindings;
        bindings.inputs.reserve(worker.inputs.size());
        bindings.outputs.reserve(worker.outputs.size());
        bindings.ports = worker.prepared.ports;

        for (const auto queue : worker.inputs) {
            bindings.inputs.push_back(queues_.at(queue));
        }
        for (const auto queue : worker.outputs) {
            bindings.outputs.push_back(queues_.at(queue));
        }

        workers_.at(worker.id)->bind(std::move(bindings));
    }

    schedule_ = plan.topological_order;
}

} // namespace dataflow
