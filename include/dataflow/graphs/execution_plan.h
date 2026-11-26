#pragma once

#include <dataflow/graph_error.h>
#include <dataflow/graphs/config_graph.h>
#include <dataflow/interfaces/worker_factory.h>

#include <cstddef>
#include <vector>

namespace dataflow {

using worker_id = std::size_t;
using queue_id = std::size_t;

struct QueuePlan {
    queue_id id;
    ScalarType type;
    std::size_t capacity;
    worker_id producer;
    std::size_t producer_port;
    worker_id consumer;
    std::size_t consumer_port;
};

struct WorkerPlan {
    worker_id id;
    ConfigGraph::node_id config_node;
    PreparedWorker prepared;
    std::vector<queue_id> inputs;
    std::vector<queue_id> outputs;
};

struct ExecutionPlan {
    std::vector<WorkerPlan> workers;
    std::vector<QueuePlan> queues;
    std::vector<worker_id> topological_order;
};

class GraphCompiler {
public:
    [[nodiscard]] ExecutionPlan compile(const ConfigGraph &config,
        const IWorkerFactory &worker_factory) const;
};

} // namespace dataflow
