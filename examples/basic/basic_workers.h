#pragma once

#include <dataflow/worker.h>
#include <dataflow/worker_registry.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>

namespace example {

struct SourceConfig {
    std::size_t transaction_size = 4;
    std::size_t transactions = 4;
    std::int32_t value = 1;
};

class Source final : public dataflow::SourceFacade<SourceConfig, std::int32_t> {
    SourceConfig config_;
    std::size_t emitted_{};

public:
    static void validate(const SourceConfig &config) {
        if (config.transaction_size == 0 || config.transactions == 0) {
            throw std::invalid_argument("source sizes must be positive");
        }
    }

    static dataflow::WorkerPorts ports(const SourceConfig &config) {
        return {{},
            {dataflow::port<std::int32_t>("samples", config.transaction_size)}};
    }

    explicit Source(const SourceConfig &config) : config_(config) {
    }

protected:
    [[nodiscard]] bool can_process() const noexcept override {
        return emitted_ < config_.transactions;
    }

    void work_one(std::span<input_view>,
        std::span<output_view> outputs) override {
        outputs[0].for_each([this](auto &value) { value = config_.value; });
        ++emitted_;
    }
};

struct GainConfig {
    std::size_t transaction_size = 4;
    std::int32_t gain = 2;
};

class Gain final
    : public dataflow::WorkerFacade<GainConfig, std::int32_t, std::int32_t> {
    std::int32_t gain_;

public:
    static void validate(const GainConfig &config) {
        if (config.transaction_size == 0) {
            throw std::invalid_argument(
                "gain transaction_size must be positive");
        }
    }

    static dataflow::WorkerPorts ports(const GainConfig &config) {
        return {
            {dataflow::port<std::int32_t>("input", config.transaction_size)},
            {dataflow::port<std::int32_t>("output", config.transaction_size)}};
    }

    explicit Gain(const GainConfig &config) : gain_(config.gain) {
    }

protected:
    void work_one(std::span<input_view> inputs,
        std::span<output_view> outputs) override {
        for (std::size_t index{}; index < inputs[0].size(); ++index) {
            outputs[0][index] = inputs[0][index] * gain_;
        }
    }
};

struct SinkConfig {
    std::size_t transaction_size = 4;
};

class Sink final : public dataflow::SinkFacade<SinkConfig, std::int32_t> {
public:
    static dataflow::WorkerPorts ports(const SinkConfig &config) {
        return {
            {dataflow::port<std::int32_t>("input", config.transaction_size)},
            {}};
    }

    explicit Sink(const SinkConfig &) {
    }

protected:
    void work_one(std::span<input_view>, std::span<output_view>) override {
    }
};

inline void register_workers(dataflow::WorkerRegistry &workers) {
    workers.register_worker<Source>("example_source");
    workers.register_worker<Gain>("example_gain");
    workers.register_worker<Sink>("example_sink");
}

} // namespace example
