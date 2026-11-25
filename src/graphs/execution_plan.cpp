#include <graphs/execution_plan.h>

#include <algorithm>
#include <exception>
#include <limits>
#include <optional>
#include <sstream>
#include <unordered_set>
#include <utility>
#include <variant>

namespace dataflow {
namespace {

constexpr auto invalid_queue = std::numeric_limits<queue_id>::max();

void add_diagnostic(std::vector<GraphDiagnostic> &diagnostics,
    std::string message) {
    diagnostics.push_back({std::move(message)});
}

std::string port_reference_name(
    const ConfigGraph::EndpointSpec::PortReference &reference) {
    return std::visit(
        [](const auto &value) -> std::string {
            using Value = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<Value, std::size_t>) {
                return "[" + std::to_string(value) + "]";
            } else {
                return "." + value;
            }
        },
        reference);
}

std::string edge_name(const ConfigGraph &config,
    const ConfigGraph::EdgeSpec &edge) {
    return config.node(edge.producer).id +
           port_reference_name(edge.output_port) + " -> " +
           config.node(edge.consumer).id + port_reference_name(edge.input_port);
}

std::optional<std::size_t> resolve_port(
    const std::vector<PortDescriptor> &ports,
    const ConfigGraph::EndpointSpec::PortReference &reference) {
    if (const auto *index = std::get_if<std::size_t>(&reference)) {
        if (*index < ports.size()) {
            return *index;
        }
        return std::nullopt;
    }

    const auto &name = std::get<std::string>(reference);
    const auto found = std::ranges::find(ports, name, &PortDescriptor::name);
    if (found == ports.end()) {
        return std::nullopt;
    }
    return static_cast<std::size_t>(found - ports.begin());
}

void validate_port_group(std::vector<GraphDiagnostic> &diagnostics,
    std::string_view worker,
    std::string_view direction,
    const std::vector<PortDescriptor> &ports) {
    std::unordered_set<std::string> names;
    for (std::size_t index{}; index < ports.size(); ++index) {
        const auto &port = ports[index];
        const auto prefix = "worker '" + std::string(worker) + "' " +
                            std::string(direction) + "[" +
                            std::to_string(index) + "]";
        if (port.name.empty()) {
            add_diagnostic(diagnostics, prefix + " has an empty name");
        } else if (!names.insert(port.name).second) {
            add_diagnostic(diagnostics,
                "worker '" + std::string(worker) + "' has duplicate " +
                    std::string(direction) + " port name '" + port.name + "'");
        }
        if (port.transaction_size == 0) {
            add_diagnostic(diagnostics,
                prefix + " transaction size must be positive");
        }
    }
}

} // namespace

GraphCompilationError::GraphCompilationError(
    std::vector<GraphDiagnostic> diagnostics)
    : std::runtime_error("execution graph compilation failed"),
      diagnostics_(std::move(diagnostics)) {
}

ExecutionPlan GraphCompiler::compile(const ConfigGraph &config,
    const IWorkerFactory &worker_factory) const {
    std::vector<GraphDiagnostic> diagnostics;
    std::vector<std::optional<PreparedWorker>> prepared(config.node_amount());

    for (ConfigGraph::node_id id{}; id < config.node_amount(); ++id) {
        const auto &node = config.node(id);
        try {
            auto worker = worker_factory.prepare(node.type, node.params);
            if (!worker.ports) {
                throw std::invalid_argument(
                    "worker factory returned null ports");
            }
            if (!worker.create) {
                throw std::invalid_argument(
                    "worker factory returned no creator");
            }
            validate_port_group(diagnostics,
                node.id,
                "input",
                worker.ports->inputs);
            validate_port_group(diagnostics,
                node.id,
                "output",
                worker.ports->outputs);
            prepared[id] = std::move(worker);
        } catch (const std::exception &error) {
            add_diagnostic(diagnostics,
                "worker '" + node.id + "': " + error.what());
        }
    }

    const auto [order_or_cycle, is_cycle] = config.sort();
    if (is_cycle) {
        std::ostringstream message;
        message << "cycle detected: ";
        for (std::size_t i{}; i < order_or_cycle.size(); ++i) {
            if (i != 0) {
                message << " -> ";
            }
            message << config.node(order_or_cycle[i]).id;
        }
        add_diagnostic(diagnostics, message.str());
    }

    if (std::ranges::any_of(prepared,
            [](const auto &worker) { return !worker.has_value(); })) {
        throw GraphCompilationError(std::move(diagnostics));
    }

    ExecutionPlan plan;
    plan.workers.reserve(config.node_amount());
    for (worker_id id{}; id < config.node_amount(); ++id) {
        auto worker = std::move(*prepared[id]);
        const auto input_count = worker.ports->inputs.size();
        const auto output_count = worker.ports->outputs.size();
        plan.workers.push_back({id,
            id,
            std::move(worker),
            std::vector<queue_id>(input_count, invalid_queue),
            std::vector<queue_id>(output_count, invalid_queue)});
    }

    for (const auto &edge : config.edges()) {
        const auto label = edge_name(config, edge);
        const auto &producer = *plan.workers[edge.producer].prepared.ports;
        const auto &consumer = *plan.workers[edge.consumer].prepared.ports;
        const auto output_port =
            resolve_port(producer.outputs, edge.output_port);
        const auto input_port = resolve_port(consumer.inputs, edge.input_port);
        if (!output_port) {
            add_diagnostic(diagnostics,
                label + ": producer output port does not exist");
        }
        if (!input_port) {
            add_diagnostic(diagnostics,
                label + ": consumer input port does not exist");
        }
        if (!output_port || !input_port) {
            continue;
        }

        const auto &output = producer.outputs[*output_port];
        const auto &input = consumer.inputs[*input_port];
        std::size_t capacity{};
        bool valid = true;

        if (output.transaction_size == 0) {
            add_diagnostic(diagnostics,
                label + ": producer transaction size must be positive");
            valid = false;
        }
        if (input.transaction_size == 0) {
            add_diagnostic(diagnostics,
                label + ": consumer transaction size must be positive");
            valid = false;
        }

        if (edge.capacity) {
            capacity = *edge.capacity;
        } else if (output.transaction_size >
                   std::numeric_limits<std::size_t>::max() -
                       input.transaction_size) {
            add_diagnostic(diagnostics,
                label + ": default queue capacity calculation overflowed");
            valid = false;
        } else {
            capacity = output.transaction_size + input.transaction_size;
        }

        if (output.type != input.type) {
            add_diagnostic(diagnostics,
                label + ": producer and consumer port types differ");
            valid = false;
        }
        if (edge.asserted_type && (*edge.asserted_type != output.type ||
                                      *edge.asserted_type != input.type)) {
            add_diagnostic(diagnostics,
                label + ": asserted queue type and port types differ");
            valid = false;
        }
        if (capacity == 0) {
            add_diagnostic(diagnostics,
                label + ": queue capacity must be positive");
            valid = false;
        } else if (capacity < output.transaction_size ||
                   capacity < input.transaction_size) {
            add_diagnostic(diagnostics,
                label + ": queue capacity is smaller than a transaction");
            valid = false;
        }

        auto &producer_queue =
            plan.workers[edge.producer].outputs[*output_port];
        auto &consumer_queue = plan.workers[edge.consumer].inputs[*input_port];
        if (producer_queue != invalid_queue) {
            add_diagnostic(diagnostics,
                label + ": producer output is already connected");
            valid = false;
        }
        if (consumer_queue != invalid_queue) {
            add_diagnostic(diagnostics,
                label + ": consumer input is already connected");
            valid = false;
        }

        if (!valid) {
            continue;
        }

        const auto id = plan.queues.size();
        plan.queues.push_back({id,
            output.type,
            capacity,
            edge.producer,
            *output_port,
            edge.consumer,
            *input_port});
        producer_queue = id;
        consumer_queue = id;
    }

    for (const auto &worker : plan.workers) {
        const auto &node = config.node(worker.config_node);
        for (std::size_t port{}; port < worker.inputs.size(); ++port) {
            if (worker.inputs[port] == invalid_queue) {
                add_diagnostic(diagnostics,
                    "worker '" + node.id + "' input '" +
                        worker.prepared.ports->inputs[port].name +
                        "' is not connected");
            }
        }
        for (std::size_t port{}; port < worker.outputs.size(); ++port) {
            if (worker.outputs[port] == invalid_queue) {
                add_diagnostic(diagnostics,
                    "worker '" + node.id + "' output '" +
                        worker.prepared.ports->outputs[port].name +
                        "' is not connected");
            }
        }
    }

    if (!diagnostics.empty()) {
        throw GraphCompilationError(std::move(diagnostics));
    }

    plan.topological_order.assign(order_or_cycle.begin(), order_or_cycle.end());
    return plan;
}

} // namespace dataflow
