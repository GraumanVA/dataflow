#pragma once

#include <algorithm>
#include <cstddef>
#include <functional>
#include <stdexcept>
#include <vector>

namespace dataflow {

enum class StopPolicy {
    any,
    all
};

class StopController {
    std::vector<bool> satisfied_;
    StopPolicy policy_ = StopPolicy::all;

public:
    explicit StopController(std::size_t criterion_count,
        StopPolicy policy = StopPolicy::all)
        : satisfied_(criterion_count, false), policy_(policy) {
    }

    void update(std::size_t criterion, bool satisfied) {
        if (criterion >= satisfied_.size()) {
            throw std::out_of_range("invalid stop criterion id");
        }
        satisfied_[criterion] = satisfied_[criterion] || satisfied;
    }

    [[nodiscard]] bool stop_requested() const noexcept {
        if (satisfied_.empty()) {
            return false;
        }

        if (policy_ == StopPolicy::any) {
            return std::ranges::any_of(satisfied_, std::identity{});
        }

        return std::ranges::all_of(satisfied_, std::identity{});
    }

    [[nodiscard]] const std::vector<bool> &states() const noexcept {
        return satisfied_;
    }
};

} // namespace dataflow
