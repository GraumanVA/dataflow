#include <dataflow/observer.h>
#include <dataflow/worker.h>
#include <dataflow/worker_registry.h>
#include <dataflow/worker_test.h>

#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <utility>

namespace {

struct Config {
    std::size_t transaction_size = 1;
};

class Worker final
    : public dataflow::SizedTypedWorker<std::int32_t, std::int32_t> {
public:
    using Config = ::Config;
    using Base = dataflow::SizedTypedWorker<std::int32_t, std::int32_t>;
    using typename Base::input_view;
    using typename Base::output_view;

    static dataflow::WorkerPorts ports(const Config &config) {
        return {{{"input", dataflow::ScalarType::i32, config.transaction_size}},
            {{"output", dataflow::ScalarType::i32, config.transaction_size}}};
    }

    Worker(const Config &, std::shared_ptr<const dataflow::WorkerPorts> ports)
        : Base(std::move(ports)) {
    }

    void work_one(std::span<input_view>, std::span<output_view>) override {
        throw std::logic_error("unbound worker executed");
    }
};

struct FacadeSourceConfig {
    std::size_t transaction_size = 1;
    std::size_t transactions = 1;
    std::int32_t value = 0;
};

class FacadeSource final
    : public dataflow::SourceFacade<FacadeSourceConfig, std::int32_t> {
    FacadeSourceConfig config_;
    std::size_t emitted_{};

public:
    static void validate(const FacadeSourceConfig &config) {
        if (config.transaction_size == 0 || config.transactions == 0) {
            throw std::invalid_argument("facade source sizes must be positive");
        }
    }

    static dataflow::WorkerPorts ports(const FacadeSourceConfig &config) {
        return {{},
            {dataflow::port<std::int32_t>("output", config.transaction_size)}};
    }

    explicit FacadeSource(const FacadeSourceConfig &config) : config_(config) {
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

struct TypedFacadeConfig {
    std::size_t transaction_size = 2;
    float scale = 0.5F;
};

class TypedFacade final : public dataflow::TypedWorkerFacade<TypedFacadeConfig,
                              dataflow::InputPorts<std::int32_t, float>,
                              dataflow::OutputPorts<float, std::uint8_t>> {
    float scale_;

public:
    static dataflow::WorkerPorts ports(const TypedFacadeConfig &config) {
        return {
            {dataflow::port<std::int32_t>("integers", config.transaction_size),
                dataflow::port<float>("fractions", config.transaction_size)},
            {dataflow::port<float>("scaled_sum", config.transaction_size),
                dataflow::port<std::uint8_t>("positive",
                    config.transaction_size)}};
    }

    explicit TypedFacade(const TypedFacadeConfig &config)
        : scale_(config.scale) {
    }

protected:
    void work_one(input_views &inputs, output_views &outputs) override {
        const auto &integers = std::get<0>(inputs);
        const auto &fractions = std::get<1>(inputs);
        auto &scaled_sum = std::get<0>(outputs);
        auto &positive = std::get<1>(outputs);
        for (std::size_t index{}; index < integers.size(); ++index) {
            scaled_sum[index] =
                static_cast<float>(integers[index]) * scale_ + fractions[index];
            positive[index] =
                integers[index] > 0 ? std::uint8_t{1} : std::uint8_t{0};
        }
    }
};

struct TypedObserverConfig {
    std::size_t transaction_size = 2;
};

class TypedObserverProbe final
    : public dataflow::ConfiguredTypedObserver<TypedObserverProbe,
          TypedObserverConfig,
          dataflow::InputPorts<std::int32_t, float>,
          dataflow::OutputPorts<>> {
    float total_{};

public:
    using Config = TypedObserverConfig;
    using Base = dataflow::ConfiguredTypedObserver<TypedObserverProbe,
        TypedObserverConfig,
        dataflow::InputPorts<std::int32_t, float>,
        dataflow::OutputPorts<>>;

    static dataflow::WorkerPorts ports(const TypedObserverConfig &config) {
        return {
            {dataflow::port<std::int32_t>("integers", config.transaction_size),
                dataflow::port<float>("fractions", config.transaction_size)},
            {}};
    }

    explicit TypedObserverProbe(const TypedObserverConfig &config)
        : Base(config) {
    }

    [[nodiscard]] float total() const noexcept {
        return total_;
    }

    [[nodiscard]] std::vector<dataflow::StatisticValue> statistics()
        const override {
        return {{"total", std::to_string(total_)}};
    }

    void merge_from_same(const TypedObserverProbe &other) {
        total_ += other.total_;
    }

protected:
    void work_one(input_views &inputs, output_views &) override {
        const auto &integers = std::get<0>(inputs);
        const auto &fractions = std::get<1>(inputs);
        for (std::size_t index{}; index < integers.size(); ++index) {
            total_ += static_cast<float>(integers[index]) + fractions[index];
        }
    }
};

void require(bool condition, const char *message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

} // namespace

int main() {
    const Config config;
    auto ports =
        std::make_shared<const dataflow::WorkerPorts>(Worker::ports(config));
    Worker worker(config, ports);
    require(!worker.is_ready(), "unbound worker reported ready");
    require(!worker.process_one(), "unbound worker processed a transaction");

    dataflow::WorkerRegistry registry;
    registry.register_worker<Worker>("worker");
    require(registry.contains("worker"), "registered worker was not listed");
    require(registry.registered_types() == std::vector<std::string>{"worker"},
        "registered worker type list was incorrect");
    const auto prepared =
        registry.prepare("worker", YAML::Load("transaction_size: 1"));
    require(prepared.ports->inputs.size() == 1,
        "worker without validate was not prepared");
    try {
        static_cast<void>(registry.prepare("worker",
            YAML::Load("transaction_size: 1\nunknown: true")));
        throw std::runtime_error("unknown worker parameter was accepted");
    } catch (const std::invalid_argument &) {
    }
    try {
        registry.register_worker<Worker>("worker");
        throw std::runtime_error("duplicate registration was accepted");
    } catch (const std::invalid_argument &) {
    }

    registry.register_worker<FacadeSource>("facade_source");
    const auto defaulted_facade =
        registry.prepare("facade_source", YAML::Load("transactions: 1"));
    require(defaulted_facade.ports->outputs[0].transaction_size == 1,
        "default worker parameter was not applied");
    auto default_output =
        std::make_shared<dataflow::CyclicQueue<std::int32_t>>(1, 1);
    auto defaulted_worker = defaulted_facade.create();
    defaulted_worker->bind({{}, {default_output}, defaulted_facade.ports});
    require(defaulted_worker->process_one(),
        "worker with default parameters made no progress");
    auto default_read = default_output->begin_read_exact(1);
    require(default_read.spans()[0] == 0,
        "default worker parameter had the wrong value");

    const auto facade = registry.prepare("facade_source",
        YAML::Load("transaction_size: 2\ntransactions: 1\nvalue: 7"));
    require(facade.ports->inputs.empty(),
        "facade source unexpectedly declared an input");
    require(facade.ports->outputs.size() == 1,
        "facade source output layout was not prepared");

    auto output = std::make_shared<dataflow::CyclicQueue<std::int32_t>>(2, 0);
    auto facade_worker = facade.create();
    require(!facade_worker->is_ready(), "unbound facade worker reported ready");
    facade_worker->bind({{}, {output}, facade.ports});
    require(facade_worker->process_one(),
        "facade source did not process its configured transaction");
    require(!facade_worker->process_one(),
        "facade source readiness was not composed with can_process");

    auto output_read = output->begin_read_exact(2);
    require(output_read.spans()[0] == 7 && output_read.spans()[1] == 7,
        "facade source produced incorrect output");

    dataflow::WorkerTestHarness<TypedFacade> harness(TypedFacadeConfig{});
    harness.write_input<std::int32_t>("integers", {2, -4});
    harness.write_input<float>("fractions", {0.25F, 0.5F});
    require(harness.is_ready(), "typed facade harness was not ready");
    require(harness.process_one(), "typed facade harness made no progress");
    require(harness.output_size<float>("scaled_sum") == 2,
        "typed facade harness reported the wrong output size");
    require(harness.read_output<float>("scaled_sum") ==
                std::vector<float>{1.25F, -1.5F},
        "typed facade produced incorrect float output");
    require(harness.read_output<std::uint8_t>("positive") ==
                std::vector<std::uint8_t>{1, 0},
        "typed facade produced incorrect integer output");

    dataflow::WorkerTestHarness<TypedObserverProbe> observer(
        TypedObserverConfig{});
    observer.write_input<std::int32_t>("integers", {1, 2});
    observer.write_input<float>("fractions", {0.25F, 0.75F});
    require(observer.process_one(), "typed observer facade made no progress");
    require(observer.worker().total() == 4.0F,
        "typed observer facade accumulated the wrong value");
    const auto empty = observer.worker().make_empty();
    require(empty->statistics().front().value == "0.000000",
        "typed observer facade did not create an empty aggregation instance");
}
