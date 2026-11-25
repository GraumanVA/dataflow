#pragma once

#include <interfaces/statistics.h>
#include <interfaces/worker_factory.h>

#include <concepts>
#include <functional>
#include <memory>
#include <ranges>
#include <rfl/DefaultIfMissing.hpp>
#include <rfl/yaml.hpp>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dataflow {

class WorkerRegistry final : public IWorkerFactory {
    using Preparer = std::function<PreparedWorker(const YAML::Node &params)>;

    std::unordered_map<std::string, Preparer> entries_;

    template <class Config>
    [[nodiscard]] static auto parse(const YAML::Node &params) {
        return rfl::yaml::read<Config,
            rfl::NoExtraFields,
            rfl::DefaultIfMissing>(YAML::Dump(params));
    }

public:
    template <class Config, class Implementation>
        requires std::derived_from<Implementation, IWorker> &&
                 requires(const Config &config) {
                     {
                         Implementation::ports(config)
                     } -> std::same_as<WorkerPorts>;
                 } &&
                 (std::constructible_from<Implementation,
                      const Config &,
                      std::shared_ptr<const WorkerPorts>> ||
                     (std::derived_from<Implementation, WorkerFacadeTag> &&
                         std::constructible_from<Implementation,
                             const Config &>))
    void register_type(std::string type) {
        static_assert(std::default_initializable<Config>,
            "worker Config must be default-initializable so omitted YAML "
            "fields can use C++ defaults");
        Preparer prepare = [](const YAML::Node &params) -> PreparedWorker {
            auto parsed = parse<Config>(params);
            if (!parsed) {
                throw std::invalid_argument(
                    "invalid worker parameters: " + parsed.error().what());
            }

            if constexpr (requires(const Config &config) {
                              {
                                  Implementation::validate(config)
                              } -> std::same_as<void>;
                          }) {
                Implementation::validate(*parsed);
            }

            auto config = std::make_shared<const Config>(std::move(*parsed));
            auto ports = std::make_shared<const WorkerPorts>(
                Implementation::ports(*config));

            return {ports, [config, ports]() -> std::unique_ptr<IWorker> {
                        if constexpr (std::derived_from<Implementation,
                                          WorkerFacadeTag>) {
                            return std::make_unique<Implementation>(*config);
                        } else {
                            return std::make_unique<Implementation>(*config,
                                ports);
                        }
                    }};
        };

        const auto [unused, inserted] =
            entries_.emplace(type, std::move(prepare));
        static_cast<void>(unused);
        if (!inserted) {
            throw std::invalid_argument(
                "worker type is already registered: " + type);
        }
    }

    template <class Implementation>
        requires requires { typename Implementation::Config; }
    void register_worker(std::string type) {
        static_assert(std::derived_from<Implementation, IWorker>,
            "workers must derive from IWorker");
        register_type<typename Implementation::Config, Implementation>(
            std::move(type));
    }

    template <class Implementation>
        requires std::derived_from<Implementation, IStatisticsProvider> &&
                 requires { typename Implementation::Config; }
    void register_observer(std::string type) {
        register_worker<Implementation>(std::move(type));
    }

    [[nodiscard]] bool contains(std::string_view type) const {
        return entries_.contains(std::string(type));
    }

    [[nodiscard]] std::vector<std::string> registered_types() const {
        std::vector<std::string> result;
        result.reserve(entries_.size());
        for (const auto &[type, unused] : entries_) {
            static_cast<void>(unused);
            result.push_back(type);
        }
        std::ranges::sort(result);
        return result;
    }

    [[nodiscard]] PreparedWorker prepare(std::string_view type,
        const YAML::Node &params) const override {
        const auto it = entries_.find(std::string(type));
        if (it == entries_.end()) {
            throw std::invalid_argument(
                "unknown worker type: " + std::string(type));
        }
        return std::invoke(it->second, params);
    }
};

} // namespace dataflow
