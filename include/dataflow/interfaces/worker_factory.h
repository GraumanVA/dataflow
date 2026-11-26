#pragma once

#include <dataflow/interfaces/iworker.h>
#include <dataflow/interfaces/worker_ports.h>

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <yaml-cpp/yaml.h>

namespace dataflow {

struct PreparedWorker {
    std::shared_ptr<const WorkerPorts> ports;
    std::function<std::unique_ptr<IWorker>()> create;
};

class IWorkerFactory {
public:
    virtual ~IWorkerFactory() = default;

    IWorkerFactory() = default;
    IWorkerFactory(const IWorkerFactory &) = delete;
    IWorkerFactory &operator=(const IWorkerFactory &) = delete;
    IWorkerFactory(IWorkerFactory &&) = delete;
    IWorkerFactory &operator=(IWorkerFactory &&) = delete;

    [[nodiscard]] virtual PreparedWorker prepare(std::string_view type,
        const YAML::Node &params) const = 0;
};

} // namespace dataflow
