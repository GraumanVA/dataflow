#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

namespace dataflow {

struct SweepTarget {
    std::string node;
    std::vector<std::string> path;
};

struct SweepAssignment {
    SweepTarget target;
    YAML::Node value;
};

struct SweepNamedValue {
    std::string name;
    YAML::Node value;
    std::string display_name;
};

struct ExpandedSweepPoint {
    std::size_t index;
    std::vector<SweepNamedValue> parameters;
    std::vector<SweepAssignment> assignments;
};

struct ExpandedReplicaParameters {
    std::size_t replica;
    std::vector<SweepNamedValue> parameters;
    std::vector<SweepAssignment> assignments;
};

class SweepDefinition {
public:
    enum class Combination {
        product,
        zip
    };

    enum class CaseNaming {
        index,
        parameters
    };

private:
    struct Axis {
        std::string name;
        std::string display_name;
        std::vector<YAML::Node> values;
        std::vector<SweepTarget> targets;
    };

    struct SerialParameter {
        std::string name;
        std::string display_name;
        std::int64_t start{};
        std::int64_t step = 1;
        std::vector<SweepTarget> targets;
    };

    Combination combination_ = Combination::product;
    CaseNaming case_naming_ = CaseNaming::index;
    std::vector<Axis> axes_;
    std::vector<SerialParameter> derived_;
    std::vector<SerialParameter> replica_parameters_;

public:
    SweepDefinition() = default;
    explicit SweepDefinition(const YAML::Node &node);

    [[nodiscard]] bool empty() const noexcept;
    [[nodiscard]] std::vector<ExpandedSweepPoint> expand_points() const;
    [[nodiscard]] std::vector<ExpandedReplicaParameters> expand_replicas(
        std::size_t replica_count) const;
    [[nodiscard]] std::vector<SweepTarget> targets() const;
    [[nodiscard]] std::string case_directory_name(
        const ExpandedSweepPoint &point) const;
};

} // namespace dataflow
