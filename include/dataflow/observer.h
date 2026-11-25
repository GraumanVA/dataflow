#pragma once

#include <dataflow/statistics.h>
#include <dataflow/worker.h>

#include <memory>
#include <utility>

namespace dataflow {

template <class Derived, typename Input, typename Output>
class SizedTypedObserver : public SizedTypedWorker<Input, Output>,
                           public TypedStatisticsProvider<Derived> {
protected:
    SizedTypedObserver() = default;

    explicit SizedTypedObserver(std::shared_ptr<const WorkerPorts> ports)
        : SizedTypedWorker<Input, Output>(std::move(ports)) {
    }
};

template <class Derived, class Config, typename Input, typename Output>
class ConfiguredSizedTypedObserver
    : public SizedTypedObserver<Derived, Input, Output>,
      public WorkerFacadeTag {
    Config config_;

protected:
    explicit ConfiguredSizedTypedObserver(const Config &config)
        : config_(config) {
    }

    [[nodiscard]] const Config &observer_config() const noexcept {
        return config_;
    }

    [[nodiscard]] const WorkerPorts &observer_ports() const {
        return this->worker_ports();
    }

public:
    [[nodiscard]] std::unique_ptr<Derived> make_empty_same() const {
        return std::make_unique<Derived>(config_);
    }
};

template <class Derived, class Inputs, class Outputs>
class TypedObserver : public TypedWorker<Inputs, Outputs>,
                      public TypedStatisticsProvider<Derived> {
protected:
    TypedObserver() = default;

    explicit TypedObserver(std::shared_ptr<const WorkerPorts> ports)
        : TypedWorker<Inputs, Outputs>(std::move(ports)) {
    }
};

template <class Derived, class Config, class Inputs, class Outputs>
class ConfiguredTypedObserver : public TypedObserver<Derived, Inputs, Outputs>,
                                public WorkerFacadeTag {
    Config config_;

protected:
    explicit ConfiguredTypedObserver(const Config &config) : config_(config) {
    }

    [[nodiscard]] const Config &observer_config() const noexcept {
        return config_;
    }

    [[nodiscard]] const WorkerPorts &observer_ports() const {
        return this->worker_ports();
    }

public:
    [[nodiscard]] std::unique_ptr<Derived> make_empty_same() const {
        return std::make_unique<Derived>(config_);
    }
};

} // namespace dataflow
