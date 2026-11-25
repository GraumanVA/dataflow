#pragma once

#include <graphs/sweep.h>
#include <interfaces/stop_token.h>
#include <storage/queue_types.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <istream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <yaml-cpp/yaml.h>

namespace dataflow {

class ConfigGraph {
public:
    using node_id = std::size_t;

    struct NodeSpec {
        std::string id;
        std::string type;
        YAML::Node params;
    };

    struct EndpointSpec {
        using PortReference = std::variant<std::size_t, std::string>;
        std::string node;
        PortReference port;
    };

    struct ConfiguredEdgeSpec {
        EndpointSpec producer;
        EndpointSpec consumer;
        std::optional<ScalarType> asserted_type;
        std::optional<std::size_t> capacity;
    };

    struct StopSpec {
        StopPolicy policy = StopPolicy::all;
        std::vector<std::string> observers;
    };

    struct TerminalReportSpec {
        bool enabled = false;
        double interval_seconds = 1.0;
        bool update_in_place = true;
    };

    struct ReportingSpec {
        TerminalReportSpec terminal;
    };

    struct StatisticsSpec {
        double publish_interval_seconds = 1.0;
    };

    struct RuntimeSpec {
        std::size_t threads = 1;
        StopSpec stop;
        ReportingSpec reporting;
        StatisticsSpec statistics;
    };

    struct GraphSpec {
        std::string scenario = "scenario";
        std::vector<NodeSpec> nodes;
        std::vector<ConfiguredEdgeSpec> edges;
        RuntimeSpec runtime;
        SweepDefinition sweep;
    };

    struct EdgeSpec {
        node_id producer;
        EndpointSpec::PortReference output_port;
        node_id consumer;
        EndpointSpec::PortReference input_port;
        std::optional<ScalarType> asserted_type;
        std::optional<std::size_t> capacity;
    };

private:
    enum class DFSColor : std::uint8_t {
        unvisited,
        processing,
        visited
    };

    std::vector<NodeSpec> nodes_;
    std::vector<EdgeSpec> edges_;
    std::vector<std::vector<node_id>> adj_;
    RuntimeSpec runtime_;
    std::string scenario_;
    SweepDefinition sweep_;

    [[nodiscard]] static StopPolicy parse_stop_policy(const YAML::Node &node);
    [[nodiscard]] static RuntimeSpec parse_runtime(const YAML::Node &node);
    [[nodiscard]] static ScalarType parse_scalar_type(const YAML::Node &node);
    [[nodiscard]] static EndpointSpec parse_endpoint(const YAML::Node &node,
        std::string_view field_name);
    [[nodiscard]] static GraphSpec parse_graph(std::istream &input);
    [[nodiscard]] bool dfs_topological_sort(node_id start_node,
        std::vector<DFSColor> &status,
        std::vector<node_id> &order,
        std::vector<node_id> &cycle) const;
    void init(GraphSpec spec);

public:
    explicit ConfigGraph(GraphSpec spec);
    explicit ConfigGraph(std::istream &input);
    explicit ConfigGraph(const std::filesystem::path &filepath);

    [[nodiscard]] const std::vector<NodeSpec> &nodes() const noexcept;
    [[nodiscard]] const NodeSpec &node(node_id id) const;
    [[nodiscard]] const std::vector<EdgeSpec> &edges() const noexcept;
    [[nodiscard]] const std::vector<std::vector<node_id>> &adj() const noexcept;
    [[nodiscard]] std::size_t node_amount() const noexcept;
    [[nodiscard]] const RuntimeSpec &runtime() const noexcept;
    [[nodiscard]] const std::string &scenario() const noexcept;
    [[nodiscard]] const SweepDefinition &sweep() const noexcept;
    [[nodiscard]] bool has_sweep() const noexcept;
    [[nodiscard]] ConfigGraph with_assignments(
        std::span<const SweepAssignment> assignments) const;
    [[nodiscard]] std::pair<std::vector<node_id>, bool> sort() const;
};

} // namespace dataflow
