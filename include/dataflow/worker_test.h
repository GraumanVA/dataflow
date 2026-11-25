#pragma once

#include <dataflow/worker.h>

#include <concepts>
#include <cstddef>
#include <initializer_list>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace dataflow {

struct WorkerTestHarnessOptions {
    std::size_t queue_capacity_transactions = 2;
};

template <class Implementation>
    requires std::derived_from<Implementation, IWorker> &&
             requires { typename Implementation::Config; }
class WorkerTestHarness {
public:
    using Config = typename Implementation::Config;

private:
    std::shared_ptr<const WorkerPorts> ports_;
    std::vector<std::shared_ptr<IQueue>> inputs_;
    std::vector<std::shared_ptr<IQueue>> outputs_;
    std::unique_ptr<Implementation> worker_;

    [[nodiscard]] static std::size_t queue_capacity(const PortDescriptor &port,
        const WorkerTestHarnessOptions &options) {
        if (port.transaction_size == 0) {
            throw std::invalid_argument(
                "worker test harness requires positive transaction sizes");
        }
        if (options.queue_capacity_transactions == 0) {
            throw std::invalid_argument(
                "worker test harness queue capacity must be positive");
        }
        if (port.transaction_size > std::numeric_limits<std::size_t>::max() /
                                        options.queue_capacity_transactions) {
            throw std::overflow_error(
                "worker test harness queue capacity overflowed");
        }
        return port.transaction_size * options.queue_capacity_transactions;
    }

    [[nodiscard]] static std::vector<std::shared_ptr<IQueue>> create_queues(
        const std::vector<PortDescriptor> &ports,
        const WorkerTestHarnessOptions &options,
        std::size_t &next_id) {
        const CyclicQueueFactory factory;
        std::vector<std::shared_ptr<IQueue>> result;
        result.reserve(ports.size());
        for (const auto &port : ports) {
            auto queue = factory.create(
                {next_id++, queue_capacity(port, options), port.type});
            if (!queue) {
                throw std::invalid_argument(
                    "worker test harness does not support a port scalar type");
            }
            result.push_back(std::shared_ptr<IQueue>(std::move(queue)));
        }
        return result;
    }

    [[nodiscard]] static std::size_t find_port(
        const std::vector<PortDescriptor> &ports,
        std::string_view name,
        std::string_view direction) {
        std::size_t result = ports.size();
        for (std::size_t index{}; index < ports.size(); ++index) {
            if (ports[index].name != name) {
                continue;
            }
            if (result != ports.size()) {
                throw std::invalid_argument(
                    "worker test harness has duplicate " +
                    std::string(direction) +
                    " port name: " + std::string(name));
            }
            result = index;
        }
        if (result == ports.size()) {
            throw std::out_of_range("worker test harness has no " +
                                    std::string(direction) +
                                    " port named: " + std::string(name));
        }
        return result;
    }

    template <typename T>
    [[nodiscard]] static std::shared_ptr<CyclicQueue<T>> typed_queue(
        const std::vector<std::shared_ptr<IQueue>> &queues,
        const std::vector<PortDescriptor> &ports,
        std::string_view name,
        std::string_view direction) {
        const auto index = find_port(ports, name, direction);
        if (ports[index].type != scalar_id_v<T>) {
            throw std::invalid_argument(
                "worker test harness " + std::string(direction) + " port '" +
                std::string(name) + "' was accessed with the wrong type");
        }
        auto queue = std::dynamic_pointer_cast<CyclicQueue<T>>(queues[index]);
        if (!queue) {
            throw std::logic_error(
                "worker test harness queue has the wrong dynamic type");
        }
        return queue;
    }

public:
    explicit WorkerTestHarness(const Config &config,
        WorkerTestHarnessOptions options = {}) {
        if constexpr (requires(const Config &candidate) {
                          {
                              Implementation::validate(candidate)
                          } -> std::same_as<void>;
                      }) {
            Implementation::validate(config);
        }

        ports_ =
            std::make_shared<const WorkerPorts>(Implementation::ports(config));
        std::size_t next_id{};
        inputs_ = create_queues(ports_->inputs, options, next_id);
        outputs_ = create_queues(ports_->outputs, options, next_id);

        if constexpr (std::derived_from<Implementation, WorkerFacadeTag>) {
            worker_ = std::make_unique<Implementation>(config);
        } else {
            worker_ = std::make_unique<Implementation>(config, ports_);
        }
        worker_->bind({inputs_, outputs_, ports_});
    }

    [[nodiscard]] const WorkerPorts &ports() const noexcept {
        return *ports_;
    }

    [[nodiscard]] Implementation &worker() noexcept {
        return *worker_;
    }

    [[nodiscard]] const Implementation &worker() const noexcept {
        return *worker_;
    }

    [[nodiscard]] bool is_ready() const {
        return worker_->is_ready();
    }

    bool process_one() {
        return worker_->process_one();
    }

    bool process() {
        return worker_->process();
    }

    template <typename T>
        requires std::is_trivially_copyable_v<T>
    void write_input(std::string_view name, std::span<const T> values) {
        if (values.empty()) {
            return;
        }
        auto queue = typed_queue<T>(inputs_, ports_->inputs, name, "input");
        auto transaction = queue->begin_write_exact(values.size());
        auto destination = transaction.spans();
        for (std::size_t index{}; index < values.size(); ++index) {
            destination[index] = values[index];
        }
        transaction.commit_all();
    }

    template <typename T>
        requires std::is_trivially_copyable_v<T>
    void write_input(std::string_view name, std::initializer_list<T> values) {
        write_input<T>(name, std::span<const T>(values.begin(), values.size()));
    }

    template <typename T>
        requires std::is_trivially_copyable_v<T>
    [[nodiscard]] std::vector<T> read_output(std::string_view name,
        std::size_t count) {
        if (count == 0) {
            return {};
        }
        auto queue = typed_queue<T>(outputs_, ports_->outputs, name, "output");
        auto transaction = queue->begin_read_exact(count);
        const auto source = transaction.spans();
        std::vector<T> result(count);
        for (std::size_t index{}; index < count; ++index) {
            result[index] = source[index];
        }
        transaction.consume_all();
        return result;
    }

    template <typename T>
        requires std::is_trivially_copyable_v<T>
    [[nodiscard]] std::vector<T> read_output(std::string_view name) {
        auto queue = typed_queue<T>(outputs_, ports_->outputs, name, "output");
        return read_output<T>(name, queue->elems_stored());
    }

    template <typename T>
        requires std::is_trivially_copyable_v<T>
    [[nodiscard]] std::size_t output_size(std::string_view name) const {
        return typed_queue<T>(outputs_, ports_->outputs, name, "output")
            ->elems_stored();
    }
};

} // namespace dataflow
