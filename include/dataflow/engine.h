#pragma once

#include <dataflow/graph_error.h>
#include <dataflow/run_result.h>
#include <dataflow/sweep_result.h>
#include <dataflow/worker_registry.h>

#include <filesystem>
#include <iosfwd>
#include <memory>

namespace dataflow {

struct EngineOptions {
    std::filesystem::path statistics_root = "statistics";
    std::ostream *terminal = nullptr;
    bool register_builtins = true;
};

class Engine {
    class Impl;
    std::unique_ptr<Impl> impl_;

public:
    explicit Engine(EngineOptions options = {});
    ~Engine();

    Engine(const Engine &) = delete;
    Engine &operator=(const Engine &) = delete;
    Engine(Engine &&) noexcept;
    Engine &operator=(Engine &&) noexcept;

    [[nodiscard]] WorkerRegistry &workers() noexcept;

    [[nodiscard]] RunResult run(const std::filesystem::path &scenario) const;
    [[nodiscard]] SweepResult run_sweep(
        const std::filesystem::path &scenario) const;
};

} // namespace dataflow
