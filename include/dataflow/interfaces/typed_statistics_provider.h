#pragma once

#include <dataflow/interfaces/statistics.h>

#include <stdexcept>

namespace dataflow {

template <class Derived>
class TypedStatisticsProvider : public IStatisticsProvider {
public:
    void merge_from(const IStatisticsProvider &other) final {
        const auto *typed = dynamic_cast<const Derived *>(&other);
        if (typed == nullptr) {
            throw std::invalid_argument(
                "cannot merge different statistics provider types");
        }
        static_cast<Derived *>(this)->merge_from_same(*typed);
    }

    [[nodiscard]] std::unique_ptr<IStatisticsProvider> make_empty()
        const final {
        return static_cast<const Derived *>(this)->make_empty_same();
    }
};

} // namespace dataflow
