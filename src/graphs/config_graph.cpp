#include <dataflow/graphs/config_graph.h>

#include <algorithm>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <ranges>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace dataflow {
namespace {

void reject_unknown_fields(const YAML::Node &node,
    std::initializer_list<std::string_view> allowed,
    std::string_view context) {
    for (const auto &field : node) {
        const auto name = field.first.as<std::string>();
        if (std::ranges::find(allowed, name) == allowed.end()) {
            throw std::invalid_argument(std::string(context) +
                                        " contains unknown field '" + name +
                                        "'");
        }
    }
}

void set_parameter(YAML::Node &params,
    const std::vector<std::string> &path,
    const YAML::Node &value) {
    if (!params || params.IsNull()) {
        params = YAML::Node(YAML::NodeType::Map);
    }
    if (!params.IsMap()) {
        throw std::invalid_argument(
            "cannot apply a sweep parameter to non-mapping worker params");
    }

    YAML::Node cursor = params;
    for (std::size_t index{}; index < path.size(); ++index) {
        const auto &field = path[index];
        if (index + 1 == path.size()) {
            cursor[field] = YAML::Clone(value);
            return;
        }
        auto child = cursor[field];
        if (!child || child.IsNull()) {
            cursor[field] = YAML::Node(YAML::NodeType::Map);
        } else if (!child.IsMap()) {
            throw std::invalid_argument(
                "sweep target path crosses a non-mapping parameter");
        }
        cursor.reset(cursor[field]);
    }
}

} // namespace

StopPolicy ConfigGraph::parse_stop_policy(const YAML::Node &node) {
    const auto value = node.as<std::string>();
    if (value == "any") {
        return StopPolicy::any;
    }
    if (value == "all") {
        return StopPolicy::all;
    }
    throw std::invalid_argument("stop policy must be 'any' or 'all'");
}

ConfigGraph::RuntimeSpec ConfigGraph::parse_runtime(const YAML::Node &node) {
    RuntimeSpec runtime;
    if (!node) {
        return runtime;
    }
    if (!node.IsMap()) {
        throw std::invalid_argument("runtime must be a mapping");
    }
    reject_unknown_fields(node,
        {"threads", "stop", "reporting", "statistics"},
        "runtime");

    if (node["threads"]) {
        runtime.threads = node["threads"].as<std::size_t>();
    }
    if (runtime.threads == 0) {
        throw std::invalid_argument("runtime threads must be positive");
    }

    if (const auto stop = node["stop"]) {
        if (!stop.IsMap()) {
            throw std::invalid_argument("runtime.stop must be a mapping");
        }
        reject_unknown_fields(stop, {"policy", "observers"}, "runtime.stop");
        if (stop["policy"]) {
            runtime.stop.policy = parse_stop_policy(stop["policy"]);
        }
        if (const auto observers = stop["observers"]) {
            if (!observers.IsSequence()) {
                throw std::invalid_argument(
                    "runtime.stop.observers must be a sequence");
            }
            for (const auto &observer : observers) {
                runtime.stop.observers.push_back(observer.as<std::string>());
            }
        }
    }

    if (const auto reporting = node["reporting"]) {
        if (!reporting.IsMap()) {
            throw std::invalid_argument("runtime.reporting must be a mapping");
        }
        reject_unknown_fields(reporting, {"terminal"}, "runtime.reporting");
        if (const auto terminal = reporting["terminal"]) {
            if (!terminal.IsMap()) {
                throw std::invalid_argument(
                    "runtime.reporting.terminal must be a mapping");
            }
            reject_unknown_fields(terminal,
                {"enabled", "interval_seconds", "update_in_place"},
                "runtime.reporting.terminal");
            if (terminal["enabled"]) {
                runtime.reporting.terminal.enabled =
                    terminal["enabled"].as<bool>();
            }
            if (terminal["interval_seconds"]) {
                runtime.reporting.terminal.interval_seconds =
                    terminal["interval_seconds"].as<double>();
            }
            if (terminal["update_in_place"]) {
                runtime.reporting.terminal.update_in_place =
                    terminal["update_in_place"].as<bool>();
            }
            if (runtime.reporting.terminal.interval_seconds < 0.0) {
                throw std::invalid_argument(
                    "terminal report interval must not be negative");
            }
        }
    }

    if (const auto statistics = node["statistics"]) {
        if (!statistics.IsMap()) {
            throw std::invalid_argument("runtime.statistics must be a mapping");
        }
        reject_unknown_fields(statistics,
            {"publish_interval_seconds"},
            "runtime.statistics");
        if (statistics["publish_interval_seconds"]) {
            runtime.statistics.publish_interval_seconds =
                statistics["publish_interval_seconds"].as<double>();
        }
        if (runtime.statistics.publish_interval_seconds < 0.0) {
            throw std::invalid_argument(
                "statistics publish interval must not be negative");
        }
    }
    return runtime;
}

ScalarType ConfigGraph::parse_scalar_type(const YAML::Node &node) {
    const auto name = node.as<std::string>();
    const auto type = scalar_type_from_name(name);
    if (!type) {
        throw std::invalid_argument("unknown scalar type: " + name);
    }
    return *type;
}

ConfigGraph::EndpointSpec ConfigGraph::parse_endpoint(const YAML::Node &node,
    std::string_view field_name) {
    if (!node || !node.IsMap()) {
        throw std::invalid_argument(
            std::string(field_name) + " must be a mapping");
    }
    reject_unknown_fields(node, {"node", "port"}, field_name);
    if (!node["node"] || !node["port"]) {
        throw std::invalid_argument(
            std::string(field_name) + " requires node and port fields");
    }

    const auto text = node["port"].as<std::string>();
    const auto numeric =
        !text.empty() && std::ranges::all_of(text, [](unsigned char ch) {
            return ch >= '0' && ch <= '9';
        });
    if (numeric) {
        return {node["node"].as<std::string>(), node["port"].as<std::size_t>()};
    }
    if (text.empty()) {
        throw std::invalid_argument(
            std::string(field_name) + " port must not be empty");
    }
    return {node["node"].as<std::string>(), text};
}

ConfigGraph::GraphSpec ConfigGraph::parse_graph(std::istream &input) {
    try {
        const auto root = YAML::Load(input);
        if (!root || !root.IsMap()) {
            throw std::invalid_argument("graph root must be a mapping");
        }
        reject_unknown_fields(root,
            {"scenario", "runtime", "nodes", "edges", "sweep"},
            "graph");

        const auto yaml_nodes = root["nodes"];
        const auto yaml_edges = root["edges"];
        if (!yaml_nodes || !yaml_nodes.IsSequence()) {
            throw std::invalid_argument("nodes must be a sequence");
        }
        if (!yaml_edges || !yaml_edges.IsSequence()) {
            throw std::invalid_argument("edges must be a sequence");
        }

        GraphSpec graph;
        if (root["scenario"]) {
            graph.scenario = root["scenario"].as<std::string>();
        }
        if (graph.scenario.empty()) {
            throw std::invalid_argument("scenario must not be empty");
        }
        graph.runtime = parse_runtime(root["runtime"]);
        graph.sweep = SweepDefinition(root["sweep"]);
        graph.nodes.reserve(yaml_nodes.size());
        graph.edges.reserve(yaml_edges.size());

        for (const auto &node : yaml_nodes) {
            if (!node.IsMap() || !node["id"] || !node["type"]) {
                throw std::invalid_argument(
                    "each worker requires id and type fields");
            }
            reject_unknown_fields(node, {"id", "type", "params"}, "worker");
            auto params = node["params"];
            if (!params) {
                params = YAML::Node(YAML::NodeType::Map);
            }
            graph.nodes.push_back({node["id"].as<std::string>(),
                node["type"].as<std::string>(),
                std::move(params)});
        }

        for (const auto &edge : yaml_edges) {
            if (!edge.IsMap() || !edge["producer"] || !edge["consumer"]) {
                throw std::invalid_argument(
                    "each edge requires producer and consumer fields");
            }
            reject_unknown_fields(edge,
                {"producer", "consumer", "type", "capacity"},
                "edge");
            std::optional<std::size_t> capacity;
            if (edge["capacity"]) {
                capacity = edge["capacity"].as<std::size_t>();
            }
            std::optional<ScalarType> asserted_type;
            if (edge["type"]) {
                asserted_type = parse_scalar_type(edge["type"]);
            }
            graph.edges.push_back({parse_endpoint(edge["producer"], "producer"),
                parse_endpoint(edge["consumer"], "consumer"),
                asserted_type,
                capacity});
        }
        return graph;
    } catch (const YAML::Exception &error) {
        throw std::invalid_argument(
            "invalid graph YAML: " + std::string(error.what()));
    }
}

bool ConfigGraph::dfs_topological_sort(node_id start_node,
    std::vector<DFSColor> &status,
    std::vector<node_id> &order,
    std::vector<node_id> &cycle) const {
    struct Frame {
        node_id node;
        std::size_t next_neighbor{};
    };
    std::vector<Frame> stack{{start_node, 0}};
    status[start_node] = DFSColor::processing;
    while (!stack.empty()) {
        auto &frame = stack.back();
        const auto current = frame.node;
        if (frame.next_neighbor == adj_[current].size()) {
            status[current] = DFSColor::visited;
            order.push_back(current);
            stack.pop_back();
            continue;
        }
        const auto next = adj_[current][frame.next_neighbor++];
        if (status[next] == DFSColor::unvisited) {
            status[next] = DFSColor::processing;
            stack.push_back({next, 0});
            continue;
        }
        if (status[next] == DFSColor::processing) {
            const auto first = std::ranges::find(stack, next, &Frame::node);
            cycle.clear();
            std::ranges::transform(first,
                stack.end(),
                std::back_inserter(cycle),
                [](const Frame &candidate) { return candidate.node; });
            cycle.push_back(next);
            return true;
        }
    }
    return false;
}

void ConfigGraph::init(GraphSpec spec) {
    std::unordered_map<std::string, node_id> ids;
    ids.reserve(spec.nodes.size());
    for (node_id id{}; id < spec.nodes.size(); ++id) {
        if (spec.nodes[id].id.empty()) {
            throw std::invalid_argument("worker id must not be empty");
        }
        const auto [unused, inserted] = ids.emplace(spec.nodes[id].id, id);
        static_cast<void>(unused);
        if (!inserted) {
            throw std::invalid_argument(
                "duplicate worker id: " + spec.nodes[id].id);
        }
    }

    std::vector<EdgeSpec> resolved_edges;
    resolved_edges.reserve(spec.edges.size());
    for (const auto &edge : spec.edges) {
        const auto producer = ids.find(edge.producer.node);
        const auto consumer = ids.find(edge.consumer.node);
        if (producer == ids.end()) {
            throw std::invalid_argument(
                "unknown producer: " + edge.producer.node);
        }
        if (consumer == ids.end()) {
            throw std::invalid_argument(
                "unknown consumer: " + edge.consumer.node);
        }
        resolved_edges.push_back({producer->second,
            edge.producer.port,
            consumer->second,
            edge.consumer.port,
            edge.asserted_type,
            edge.capacity});
    }

    nodes_ = std::move(spec.nodes);
    edges_ = std::move(resolved_edges);
    runtime_ = std::move(spec.runtime);
    scenario_ = std::move(spec.scenario);
    sweep_ = std::move(spec.sweep);
    adj_.assign(nodes_.size(), {});
    for (const auto &edge : edges_) {
        adj_[edge.producer].push_back(edge.consumer);
    }

    for (const auto &target : sweep_.targets()) {
        if (!ids.contains(target.node)) {
            throw std::invalid_argument(
                "sweep target refers to unknown worker: " + target.node);
        }
    }
}

ConfigGraph::ConfigGraph(GraphSpec spec) {
    init(std::move(spec));
}

ConfigGraph::ConfigGraph(std::istream &input) {
    init(parse_graph(input));
}

ConfigGraph::ConfigGraph(const std::filesystem::path &filepath) {
    std::ifstream input(filepath);
    if (!input) {
        throw std::invalid_argument(
            "the file " + filepath.string() + " could not be opened");
    }
    init(parse_graph(input));
}

const std::vector<ConfigGraph::NodeSpec> &ConfigGraph::nodes() const noexcept {
    return nodes_;
}

const ConfigGraph::NodeSpec &ConfigGraph::node(node_id id) const {
    return nodes_.at(id);
}

const std::vector<ConfigGraph::EdgeSpec> &ConfigGraph::edges() const noexcept {
    return edges_;
}

const std::vector<std::vector<ConfigGraph::node_id>> &ConfigGraph::adj()
    const noexcept {
    return adj_;
}

std::size_t ConfigGraph::node_amount() const noexcept {
    return nodes_.size();
}

const ConfigGraph::RuntimeSpec &ConfigGraph::runtime() const noexcept {
    return runtime_;
}

const std::string &ConfigGraph::scenario() const noexcept {
    return scenario_;
}

const SweepDefinition &ConfigGraph::sweep() const noexcept {
    return sweep_;
}

bool ConfigGraph::has_sweep() const noexcept {
    return !sweep_.empty();
}

ConfigGraph ConfigGraph::with_assignments(
    std::span<const SweepAssignment> assignments) const {
    ConfigGraph result = *this;
    result.sweep_ = SweepDefinition{};
    for (auto &node : result.nodes_) {
        node.params = YAML::Clone(node.params);
    }

    for (const auto &assignment : assignments) {
        const auto found = std::ranges::find(result.nodes_,
            assignment.target.node,
            &NodeSpec::id);
        if (found == result.nodes_.end()) {
            throw std::invalid_argument(
                "sweep target refers to unknown worker: " +
                assignment.target.node);
        }
        set_parameter(found->params, assignment.target.path, assignment.value);
    }
    return result;
}

std::pair<std::vector<ConfigGraph::node_id>, bool> ConfigGraph::sort() const {
    std::vector<DFSColor> status(nodes_.size(), DFSColor::unvisited);
    std::vector<node_id> order;
    std::vector<node_id> cycle;
    order.reserve(nodes_.size());
    for (node_id id{}; id < nodes_.size(); ++id) {
        if (status[id] == DFSColor::unvisited &&
            dfs_topological_sort(id, status, order, cycle)) {
            return {std::move(cycle), true};
        }
    }
    std::ranges::reverse(order);
    return {std::move(order), false};
}

} // namespace dataflow
