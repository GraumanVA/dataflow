#pragma once

#include <cstddef>
#include <cstdint>
#include <random>

namespace dataflow {

struct WorkerContext {
    std::size_t sweep_point = 0;
    std::size_t replica = 0;
    std::size_t node = 0;

    [[nodiscard]] std::mt19937_64 make_random_engine(
        std::uint64_t base_seed = 0) const {
        const auto low_word = [](std::uint64_t value) {
            return static_cast<std::uint32_t>(value);
        };
        const auto high_word = [](std::uint64_t value) {
            return static_cast<std::uint32_t>(value >> 32);
        };
        std::seed_seq seed{low_word(base_seed),
            high_word(base_seed),
            low_word(sweep_point),
            high_word(sweep_point),
            low_word(replica),
            high_word(replica),
            low_word(node),
            high_word(node)};
        return std::mt19937_64{seed};
    }
};

} // namespace dataflow
