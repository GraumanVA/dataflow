#include <graphs/sweep.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <format>
#include <initializer_list>
#include <limits>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>

namespace dataflow {
namespace {

constexpr std::size_t max_sweep_points = 1'000'000;

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

[[nodiscard]] std::optional<std::int64_t> parse_integer(
    const YAML::Node &node) {
    if (!node.IsScalar()) {
        return std::nullopt;
    }
    const std::string& text = node.Scalar();
    std::int64_t value{};
    const auto [end, error] =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size()) {
        return std::nullopt;
    }
    return value;
}

[[nodiscard]] double parse_double(const YAML::Node &node,
    std::string_view field) {
    if (!node || !node.IsScalar()) {
        throw std::invalid_argument(std::string(field) + " must be a number");
    }
    try {
        const auto value = node.as<double>();
        if (!std::isfinite(value)) {
            throw std::invalid_argument(std::string(field) + " must be finite");
        }
        return value;
    } catch (const YAML::Exception &) {
        throw std::invalid_argument(std::string(field) + " must be a number");
    }
}

[[nodiscard]] std::optional<std::int64_t> checked_add(std::int64_t left,
    std::int64_t right) {
    if (right > 0 && left > std::numeric_limits<std::int64_t>::max() - right) {
        return std::nullopt;
    }
    if (right < 0 && left < std::numeric_limits<std::int64_t>::min() - right) {
        return std::nullopt;
    }
    return left + right;
}

[[nodiscard]] std::size_t parse_count(const YAML::Node &node) {
    try {
        const auto count = node.as<std::size_t>();
        if (count == 0 || count > max_sweep_points) {
            throw std::invalid_argument(
                "sweep range count must be between 1 and 1000000");
        }
        return count;
    } catch (const YAML::Exception &) {
        throw std::invalid_argument(
            "sweep range count must be a positive integer");
    }
}

[[nodiscard]] std::vector<YAML::Node> expand_integer_range(std::int64_t start,
    std::int64_t step,
    const YAML::Node &stop_node,
    const YAML::Node &count_node) {
    if (step == 0) {
        throw std::invalid_argument("sweep range step must not be zero");
    }

    std::vector<YAML::Node> result;
    if (count_node) {
        const auto count = parse_count(count_node);
        result.reserve(count);
        auto value = start;
        for (std::size_t index{}; index < count; ++index) {
            result.emplace_back(value);
            if (index + 1 != count) {
                const auto next = checked_add(value, step);
                if (!next) {
                    throw std::overflow_error("integer sweep range overflowed");
                }
                value = *next;
            }
        }
        return result;
    }

    const auto stop = parse_integer(stop_node);
    if (!stop) {
        throw std::invalid_argument(
            "integer sweep range stop must be an integer");
    }
    if ((start < *stop && step < 0) || (start > *stop && step > 0)) {
        throw std::invalid_argument(
            "sweep range step moves away from its stop");
    }

    auto value = start;
    while ((step > 0 && value <= *stop) || (step < 0 && value >= *stop)) {
        if (result.size() == max_sweep_points) {
            throw std::invalid_argument(
                "sweep range contains more than 1000000 values");
        }
        result.emplace_back(value);
        const auto next = checked_add(value, step);
        if (!next) {
            break;
        }
        value = *next;
    }
    return result;
}

[[nodiscard]] std::vector<YAML::Node> expand_floating_range(double start,
    double step,
    const YAML::Node &stop_node,
    const YAML::Node &count_node) {
    if (step == 0.0) {
        throw std::invalid_argument("sweep range step must not be zero");
    }

    std::vector<YAML::Node> result;
    std::size_t count{};
    if (count_node) {
        count = parse_count(count_node);
    } else {
        const auto stop = parse_double(stop_node, "sweep range stop");
        if ((start < stop && step < 0.0) || (start > stop && step > 0.0)) {
            throw std::invalid_argument(
                "sweep range step moves away from its stop");
        }
        const auto distance = (stop - start) / step;
        const auto tolerance = std::numeric_limits<double>::epsilon() *
                               std::max(1.0, std::abs(distance)) * 8.0;
        const auto expanded_count =
            std::floor(distance + tolerance) + 1.0;
        if (!std::isfinite(expanded_count) || expanded_count < 1.0 ||
            expanded_count > static_cast<double>(max_sweep_points)) {
            throw std::invalid_argument(
                "sweep range contains more than 1000000 values");
        }
        count = static_cast<std::size_t>(expanded_count);
    }

    result.reserve(count);
    for (std::size_t index{}; index < count; ++index) {
        const auto value = start + static_cast<double>(index) * step;
        if (!std::isfinite(value)) {
            throw std::overflow_error("floating sweep range overflowed");
        }
        result.emplace_back(value);
    }
    return result;
}

[[nodiscard]] std::vector<YAML::Node> parse_range(const YAML::Node &node) {
    if (!node || !node.IsMap()) {
        throw std::invalid_argument("sweep range must be a mapping");
    }
    reject_unknown_fields(node,
        {"start", "stop", "step", "count"},
        "sweep range");
    if (!node["start"] || !node["step"]) {
        throw std::invalid_argument("sweep range requires start and step");
    }
    if (static_cast<bool>(node["stop"]) == static_cast<bool>(node["count"])) {
        throw std::invalid_argument(
            "sweep range requires exactly one of stop or count");
    }

    const auto start_integer = parse_integer(node["start"]);
    const auto step_integer = parse_integer(node["step"]);
    const auto stop_integer = node["stop"] ? parse_integer(node["stop"])
                                           : std::optional<std::int64_t>{};
    if (start_integer && step_integer && (!node["stop"] || stop_integer)) {
        return expand_integer_range(*start_integer,
            *step_integer,
            node["stop"],
            node["count"]);
    }
    return expand_floating_range(
        parse_double(node["start"], "sweep range start"),
        parse_double(node["step"], "sweep range step"),
        node["stop"],
        node["count"]);
}

[[nodiscard]] std::vector<YAML::Node> parse_values(const YAML::Node &node) {
    if (!node) {
        throw std::invalid_argument("sweep axis requires values");
    }
    if (node.IsSequence()) {
        if (node.size() == 0) {
            throw std::invalid_argument("sweep axis values must not be empty");
        }
        std::vector<YAML::Node> result;
        result.reserve(node.size());
        for (const auto &value : node) {
            result.push_back(YAML::Clone(value));
        }
        return result;
    }
    if (!node.IsMap()) {
        throw std::invalid_argument(
            "sweep axis values must be a sequence or range mapping");
    }
    reject_unknown_fields(node, {"range"}, "sweep axis values");
    if (!node["range"]) {
        throw std::invalid_argument("sweep axis values require range");
    }
    return parse_range(node["range"]);
}

[[nodiscard]] SweepTarget parse_target(const YAML::Node &node) {
    if (!node || !node.IsMap()) {
        throw std::invalid_argument("sweep target must be a mapping");
    }
    reject_unknown_fields(node, {"node", "path", "param"}, "sweep target");
    if (!node["node"] ||
        (static_cast<bool>(node["path"]) == static_cast<bool>(node["param"]))) {
        throw std::invalid_argument(
            "sweep target requires node and exactly one of path or param");
    }

    SweepTarget result;
    result.node = node["node"].as<std::string>();
    const auto path = node["path"] ? node["path"] : node["param"];
    if (path.IsScalar()) {
        result.path.push_back(path.as<std::string>());
    } else if (path.IsSequence()) {
        for (const auto &element : path) {
            result.path.push_back(element.as<std::string>());
        }
    } else {
        throw std::invalid_argument(
            "sweep target path must be a string or sequence");
    }
    if (result.node.empty() || result.path.empty() ||
        std::ranges::any_of(result.path,
            [](const std::string &element) { return element.empty(); })) {
        throw std::invalid_argument(
            "sweep target node and path elements must not be empty");
    }
    return result;
}

[[nodiscard]] std::vector<SweepTarget> parse_targets(const YAML::Node &node) {
    if (!node || !node.IsSequence() || node.size() == 0) {
        throw std::invalid_argument(
            "sweep parameter targets must be a non-empty sequence");
    }
    std::vector<SweepTarget> result;
    result.reserve(node.size());
    for (const auto &target : node) {
        result.push_back(parse_target(target));
    }
    return result;
}

[[nodiscard]] std::pair<std::int64_t, std::int64_t> parse_serial(
    const YAML::Node &node) {
    if (!node || !node.IsMap()) {
        throw std::invalid_argument("sweep serial values must be a mapping");
    }
    reject_unknown_fields(node, {"serial"}, "sweep serial values");
    const auto serial = node["serial"];
    if (!serial || !serial.IsMap()) {
        throw std::invalid_argument(
            "sweep serial values require a serial mapping");
    }
    reject_unknown_fields(serial, {"start", "step"}, "sweep serial");
    const auto start = serial["start"] ? parse_integer(serial["start"])
                                       : std::optional<std::int64_t>{0};
    const auto step = serial["step"] ? parse_integer(serial["step"])
                                     : std::optional<std::int64_t>{1};
    if (!start || !step) {
        throw std::invalid_argument(
            "sweep serial start and step must be integers");
    }
    if (*step == 0) {
        throw std::invalid_argument("sweep serial step must not be zero");
    }
    return {*start, *step};
}

[[nodiscard]] std::string target_key(const SweepTarget &target) {
    std::string result = target.node;
    for (const auto &element : target.path) {
        result += '\x1f';
        result += element;
    }
    return result;
}

[[nodiscard]] std::string path_component(std::string_view value) {
    constexpr std::size_t max_component_size = 40;
    std::string result;
    result.reserve(std::min(value.size(), max_component_size));
    for (const auto raw : value) {
        const auto ch = static_cast<unsigned char>(raw);
        const auto allowed = (ch >= 'a' && ch <= 'z') ||
                             (ch >= 'A' && ch <= 'Z') ||
                             (ch >= '0' && ch <= '9') || ch == '.' ||
                             ch == '-' || ch == '+';
        if (allowed) {
            result += static_cast<char>(ch);
        } else if (!result.empty() && result.back() != '_') {
            result += '_';
        }
        if (result.size() == max_component_size) {
            break;
        }
    }
    while (!result.empty() &&
           (result.back() == '_' || result.back() == '.')) {
        result.pop_back();
    }
    return result.empty() ? "value" : result;
}

} // namespace

SweepDefinition::SweepDefinition(const YAML::Node &node) {
    if (!node) {
        return;
    }
    if (!node.IsMap()) {
        throw std::invalid_argument("sweep must be a mapping");
    }
    reject_unknown_fields(node,
        {"combine",
            "case_naming",
            "axes",
            "derived",
            "replica_parameters"},
        "sweep");

    if (node["combine"]) {
        const auto combine = node["combine"].as<std::string>();
        if (combine == "product") {
            combination_ = Combination::product;
        } else if (combine == "zip") {
            combination_ = Combination::zip;
        } else {
            throw std::invalid_argument(
                "sweep combine must be 'product' or 'zip'");
        }
    }

    if (node["case_naming"]) {
        const auto naming = node["case_naming"].as<std::string>();
        if (naming == "index") {
            case_naming_ = CaseNaming::index;
        } else if (naming == "parameters") {
            case_naming_ = CaseNaming::parameters;
        } else {
            throw std::invalid_argument(
                "sweep case_naming must be 'index' or 'parameters'");
        }
    }

    const auto parse_named = [](const YAML::Node &entry,
                                 std::string_view context) {
        reject_unknown_fields(entry,
            {"name", "display_name", "values", "targets"},
            context);
        if (!entry["name"] || !entry["values"] || !entry["targets"]) {
            throw std::invalid_argument(
                std::string(context) + " requires name, values, and targets");
        }
        const auto name = entry["name"].as<std::string>();
        const auto display_name = entry["display_name"]
                                      ? entry["display_name"].as<std::string>()
                                      : name;
        if (name.empty() || display_name.empty()) {
            throw std::invalid_argument(
                std::string(context) +
                " name and display_name must not be empty");
        }
        return std::tuple{
            name, display_name, parse_targets(entry["targets"])};
    };

    if (const auto axes = node["axes"]) {
        if (!axes.IsSequence()) {
            throw std::invalid_argument("sweep axes must be a sequence");
        }
        for (const auto &entry : axes) {
            if (!entry.IsMap()) {
                throw std::invalid_argument("sweep axis must be a mapping");
            }
            auto [name, display_name, targets] =
                parse_named(entry, "sweep axis");
            axes_.push_back({std::move(name),
                std::move(display_name),
                parse_values(entry["values"]),
                std::move(targets)});
        }
    }

    const auto parse_serial_parameters = [&](const YAML::Node &entries,
                                             std::string_view context,
                                             auto &destination) {
        if (!entries) {
            return;
        }
        if (!entries.IsSequence()) {
            throw std::invalid_argument(
                std::string(context) + " must be a sequence");
        }
        for (const auto &entry : entries) {
            if (!entry.IsMap()) {
                throw std::invalid_argument(
                    std::string(context) + " entry must be a mapping");
            }
            auto [name, display_name, targets] =
                parse_named(entry, context);
            const auto [start, step] = parse_serial(entry["values"]);
            destination.push_back({std::move(name),
                std::move(display_name),
                start,
                step,
                std::move(targets)});
        }
    };
    parse_serial_parameters(node["derived"], "sweep derived", derived_);
    parse_serial_parameters(node["replica_parameters"],
        "sweep replica parameter",
        replica_parameters_);

    std::unordered_set<std::string> names;
    std::unordered_set<std::string> assigned_targets;
    const auto validate_parameter = [&](const auto &parameter) {
        if (!names.insert(parameter.name).second) {
            throw std::invalid_argument(
                "duplicate sweep parameter name: " + parameter.name);
        }
        for (const auto &target : parameter.targets) {
            if (!assigned_targets.insert(target_key(target)).second) {
                throw std::invalid_argument(
                    "multiple sweep parameters assign the same target");
            }
        }
    };
    std::ranges::for_each(axes_, validate_parameter);
    std::ranges::for_each(derived_, validate_parameter);
    std::ranges::for_each(replica_parameters_, validate_parameter);

    if (axes_.size() > 1 && !node["combine"]) {
        throw std::invalid_argument(
            "sweep combine is required when multiple axes are declared");
    }
}

bool SweepDefinition::empty() const noexcept {
    return axes_.empty() && derived_.empty() && replica_parameters_.empty();
}

std::vector<ExpandedSweepPoint> SweepDefinition::expand_points() const {
    std::size_t point_count = 1;
    if (combination_ == Combination::product) {
        for (const auto &axis : axes_) {
            if (point_count > max_sweep_points / axis.values.size()) {
                throw std::invalid_argument(
                    "sweep expands to more than 1000000 points");
            }
            point_count *= axis.values.size();
        }
    } else if (!axes_.empty()) {
        point_count = axes_.front().values.size();
        if (std::ranges::any_of(axes_, [point_count](const Axis &axis) {
                return axis.values.size() != point_count;
            })) {
            throw std::invalid_argument(
                "zipped sweep axes must contain the same number of values");
        }
    }

    std::vector<ExpandedSweepPoint> result;
    result.reserve(point_count);
    std::vector<std::int64_t> derived_values;
    derived_values.reserve(derived_.size());
    for (const auto &parameter : derived_) {
        derived_values.push_back(parameter.start);
    }

    for (std::size_t point_index{}; point_index < point_count; ++point_index) {
        ExpandedSweepPoint point{point_index, {}, {}};
        std::vector<std::size_t> value_indices(axes_.size());
        if (combination_ == Combination::zip) {
            std::ranges::fill(value_indices, point_index);
        } else {
            auto remaining = point_index;
            for (std::size_t axis = axes_.size(); axis > 0; --axis) {
                value_indices[axis - 1] =
                    remaining % axes_[axis - 1].values.size();
                remaining /= axes_[axis - 1].values.size();
            }
        }

        for (std::size_t axis_index{}; axis_index < axes_.size();
            ++axis_index) {
            const auto &axis = axes_[axis_index];
            const auto &value = axis.values[value_indices[axis_index]];
            point.parameters.push_back(
                {axis.name, YAML::Clone(value), axis.display_name});
            for (const auto &target : axis.targets) {
                point.assignments.push_back({target, YAML::Clone(value)});
            }
        }
        for (std::size_t parameter_index{}; parameter_index < derived_.size();
            ++parameter_index) {
            const auto &parameter = derived_[parameter_index];
            YAML::Node value(derived_values[parameter_index]);
            point.parameters.push_back(
                {parameter.name, YAML::Clone(value), parameter.display_name});
            for (const auto &target : parameter.targets) {
                point.assignments.push_back({target, YAML::Clone(value)});
            }
            if (point_index + 1 != point_count) {
                const auto next = checked_add(derived_values[parameter_index],
                    parameter.step);
                if (!next) {
                    throw std::overflow_error(
                        "point-derived sweep serial overflowed");
                }
                derived_values[parameter_index] = *next;
            }
        }
        result.push_back(std::move(point));
    }
    return result;
}

std::vector<ExpandedReplicaParameters> SweepDefinition::expand_replicas(
    std::size_t replica_count) const {
    std::vector<ExpandedReplicaParameters> result;
    result.reserve(replica_count);
    std::vector<std::int64_t> values;
    values.reserve(replica_parameters_.size());
    for (const auto &parameter : replica_parameters_) {
        values.push_back(parameter.start);
    }

    for (std::size_t replica{}; replica < replica_count; ++replica) {
        ExpandedReplicaParameters parameters{replica, {}, {}};
        for (std::size_t index{}; index < replica_parameters_.size(); ++index) {
            const auto &definition = replica_parameters_[index];
            YAML::Node value(values[index]);
            parameters.parameters.push_back(
                {definition.name,
                    YAML::Clone(value),
                    definition.display_name});
            for (const auto &target : definition.targets) {
                parameters.assignments.push_back({target, YAML::Clone(value)});
            }
            if (replica + 1 != replica_count) {
                const auto next = checked_add(values[index], definition.step);
                if (!next) {
                    throw std::overflow_error(
                        "replica-derived sweep serial overflowed");
                }
                values[index] = *next;
            }
        }
        result.push_back(std::move(parameters));
    }
    return result;
}

std::vector<SweepTarget> SweepDefinition::targets() const {
    std::vector<SweepTarget> result;
    const auto append = [&](const auto &parameters) {
        for (const auto &parameter : parameters) {
            result.insert(result.end(),
                parameter.targets.begin(),
                parameter.targets.end());
        }
    };
    append(axes_);
    append(derived_);
    append(replica_parameters_);
    return result;
}

std::string SweepDefinition::case_directory_name(
    const ExpandedSweepPoint &point) const {
    constexpr std::size_t max_directory_name_size = 180;
    auto result = std::format("case_{:04}", point.index);
    if (case_naming_ == CaseNaming::index || point.parameters.empty()) {
        return result;
    }
    for (const auto &parameter : point.parameters) {
        result += "__";
        result += path_component(parameter.display_name);
        result += '=';
        result += path_component(YAML::Dump(parameter.value));
        if (result.size() > max_directory_name_size) {
            result.resize(max_directory_name_size);
            while (result.back() == '.') {
                result.pop_back();
            }
            break;
        }
    }
    return result;
}

} // namespace dataflow
