#pragma once

#include <dataflow/observer.h>
#include <dataflow/worker.h>
#include <dataflow/worker_registry.h>
#include <workers/ber_observer.h>
#include <workers/broadcaster.h>

#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <random>
#include <span>
#include <stdexcept>

namespace example::bsc {

struct RandomBitGeneratorConfig {
    std::uint64_t seed = 0;
    std::size_t emitted_block_size = 1024;
    double probability_one = 0.5;
};

template <std::integral T>
class RandomBitGenerator final
    : public dataflow::SourceFacade<RandomBitGeneratorConfig, T> {
    using Base = dataflow::SourceFacade<RandomBitGeneratorConfig, T>;
    using typename Base::input_view;
    using typename Base::output_view;

    std::mt19937_64 generator_;
    std::bernoulli_distribution one_;

public:
    static void validate(const RandomBitGeneratorConfig &config) {
        if (config.emitted_block_size == 0) {
            throw std::invalid_argument(
                "random bit generator block size must be positive");
        }
        if (!std::isfinite(config.probability_one) ||
            config.probability_one < 0.0 || config.probability_one > 1.0) {
            throw std::invalid_argument(
                "random bit probability must be between zero and one");
        }
    }

    [[nodiscard]] static dataflow::WorkerPorts ports(
        const RandomBitGeneratorConfig &config) {
        return {{},
            {dataflow::port<T>("bits", config.emitted_block_size)}};
    }

    explicit RandomBitGenerator(const RandomBitGeneratorConfig &config)
        : generator_(config.seed), one_(config.probability_one) {
    }

protected:
    void work_one(std::span<input_view>,
        std::span<output_view> outputs) override {
        outputs[0].for_each([this](T &bit) {
            bit = one_(generator_) ? T{1} : T{0};
        });
    }
};

struct BscChannelConfig {
    double crossover_probability = 0.01;
    std::size_t accepted_size = 1024;
};

template <std::integral T>
class BscChannel final
    : public dataflow::WorkerFacade<BscChannelConfig, T, T> {
    using Base = dataflow::WorkerFacade<BscChannelConfig, T, T>;
    using typename Base::input_view;
    using typename Base::output_view;

    std::mt19937_64 generator_{0x0ULL};
    std::bernoulli_distribution cross_;

public:
    static void validate(const BscChannelConfig &config) {
        if (config.accepted_size == 0) {
            throw std::invalid_argument(
                "BSC accepted size must be positive");
        }
        if (!std::isfinite(config.crossover_probability) ||
            config.crossover_probability < 0.0 ||
            config.crossover_probability > 1.0) {
            throw std::invalid_argument(
                "BSC crossover probability must be between zero and one");
        }
    }

    [[nodiscard]] static dataflow::WorkerPorts ports(
        const BscChannelConfig &config) {
        return {{dataflow::port<T>("input", config.accepted_size)},
            {dataflow::port<T>("output", config.accepted_size)}};
    }

    explicit BscChannel(const BscChannelConfig &config)
        : cross_(config.crossover_probability) {
    }

protected:
    void work_one(std::span<input_view> inputs,
        std::span<output_view> outputs) override {
        for (std::size_t index{}; index < inputs[0].size(); ++index) {
            outputs[0][index] = cross_(generator_)
                                    ? static_cast<T>(inputs[0][index] ^ T{1})
                                    : inputs[0][index];
        }
    }
};

inline void register_workers(dataflow::WorkerRegistry &workers) {
    workers.register_worker<RandomBitGenerator<std::int8_t>>(
        "random_bits_i8");
    workers.register_worker<BscChannel<std::int8_t>>("bsc_i8");
    workers.register_worker<dataflow::Broadcaster<std::int8_t>>(
        "broadcast_i8");
    workers.register_observer<dataflow::BerObserver<std::int8_t>>("ber_i8");
}

} // namespace example::bsc
