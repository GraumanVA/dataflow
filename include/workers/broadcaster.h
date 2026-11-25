#pragma once

#include <interfaces/iworker.h>
#include <interfaces/worker_factory.h>

#include <cstddef>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace dataflow {

struct BroadcasterConfig {
    std::size_t transaction_size = 1;
    std::vector<std::string> delivery;
};

enum class DeliveryMode {
    reliable,
    best_effort
};

template <typename T>
    requires std::is_trivially_copyable_v<T>
class Broadcaster final : public IWorker {
    std::shared_ptr<const WorkerPorts> ports_;
    std::vector<DeliveryMode> delivery_;
    std::shared_ptr<CyclicQueue<T>> input_;
    std::vector<std::shared_ptr<CyclicQueue<T>>> outputs_;
    std::vector<WriteTransaction<T>> writes_;

    void clear_workspace() noexcept {
        writes_.clear();
    }

    static DeliveryMode parse_delivery(const std::string &value) {
        if (value == "reliable") {
            return DeliveryMode::reliable;
        }
        if (value == "best_effort") {
            return DeliveryMode::best_effort;
        }
        throw std::invalid_argument(
            "broadcaster delivery must be 'reliable' or 'best_effort'");
    }

    static std::shared_ptr<CyclicQueue<T>> cast_queue(
        const std::shared_ptr<IQueue> &queue) {
        auto result = std::dynamic_pointer_cast<CyclicQueue<T>>(queue);
        if (!result) {
            throw std::invalid_argument("broadcaster queue has the wrong type");
        }
        return result;
    }

public:
    using Config = BroadcasterConfig;

    static void validate(const BroadcasterConfig &config) {
        if (config.transaction_size == 0) {
            throw std::invalid_argument(
                "broadcaster transaction size must be positive");
        }
        if (config.delivery.empty()) {
            throw std::invalid_argument(
                "broadcaster requires at least one output");
        }
        for (const auto &mode : config.delivery) {
            static_cast<void>(parse_delivery(mode));
        }
    }

    [[nodiscard]] static WorkerPorts ports(const BroadcasterConfig &config) {
        WorkerPorts result;
        result.inputs.push_back(port<T>("input", config.transaction_size));
        for (std::size_t i{}; i < config.delivery.size(); ++i) {
            result.outputs.push_back(port<T>("output_" + std::to_string(i),
                config.transaction_size));
        }
        return result;
    }

    Broadcaster(const BroadcasterConfig &config,
        std::shared_ptr<const WorkerPorts> ports)
        : ports_(std::move(ports)) {
        delivery_.reserve(config.delivery.size());
        for (const auto &mode : config.delivery) {
            delivery_.push_back(parse_delivery(mode));
        }
    }

    void bind(WorkerBindings bindings) override {
        if (bindings.inputs.size() != ports_->inputs.size() ||
            bindings.outputs.size() != ports_->outputs.size()) {
            throw std::invalid_argument(
                "broadcaster bindings do not match its configured ports");
        }
        input_ = cast_queue(bindings.inputs.front());
        outputs_.clear();
        outputs_.reserve(bindings.outputs.size());
        for (const auto &output : bindings.outputs) {
            outputs_.push_back(cast_queue(output));
        }
        clear_workspace();
        writes_.reserve(outputs_.size());
    }

    [[nodiscard]] bool is_ready() const override {
        const auto transaction_size = ports_->inputs.front().transaction_size;
        if (!input_ || input_->elems_stored() < transaction_size) {
            return false;
        }
        for (std::size_t i{}; i < outputs_.size(); ++i) {
            if (delivery_[i] == DeliveryMode::reliable &&
                outputs_[i]->elems_free() < transaction_size) {
                return false;
            }
        }
        return true;
    }

    bool process_one() override {
        if (!is_ready()) {
            return false;
        }

        clear_workspace();
        const auto transaction_size = ports_->inputs.front().transaction_size;
        auto input = input_->begin_read_exact(transaction_size);
        try {
            const auto source = input.spans();
            for (std::size_t i{}; i < outputs_.size(); ++i) {
                if (outputs_[i]->elems_free() >= transaction_size) {
                    writes_.push_back(
                        outputs_[i]->begin_write_exact(transaction_size));
                }
            }

            for (auto &write : writes_) {
                auto destination = write.spans();
                for (std::size_t element{}; element < transaction_size;
                    ++element) {
                    destination[element] = source[element];
                }
            }
            for (auto &write : writes_) {
                write.commit_all();
            }
            input.consume_all();
        } catch (...) {
            clear_workspace();
            throw;
        }
        clear_workspace();
        return true;
    }
};

} // namespace dataflow
