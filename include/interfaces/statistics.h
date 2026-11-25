#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace dataflow {

struct StatisticValue {
    std::string metric;
    std::string value;
};

class IStatisticsProvider {
public:
    virtual ~IStatisticsProvider() = default;

    [[nodiscard]] virtual std::unique_ptr<IStatisticsProvider> make_empty()
        const = 0;
    virtual void merge_from(const IStatisticsProvider &other) = 0;
    [[nodiscard]] virtual std::vector<StatisticValue> statistics() const = 0;
    [[nodiscard]] virtual bool has_completion_criterion() const noexcept {
        return false;
    }
    [[nodiscard]] virtual bool criterion_satisfied() const noexcept {
        return false;
    }
    [[nodiscard]] virtual std::string criterion_description() const {
        return {};
    }
};

} // namespace dataflow
