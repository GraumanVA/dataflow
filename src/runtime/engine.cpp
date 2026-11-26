#include <dataflow/engine.h>
#include <dataflow/runtime/coordinator.h>
#include <dataflow/storage/cyclic_queue.h>
#include <dataflow/workers/builtin_worker_registry.h>

#include <iostream>
#include <stdexcept>
#include <utility>

namespace dataflow {

class Engine::Impl {
public:
    WorkerRegistry workers;
    CyclicQueueFactory queues;
    std::filesystem::path statistics_root;
    std::ostream *terminal;

    explicit Impl(EngineOptions options)
        : statistics_root(std::move(options.statistics_root)),
          terminal(
              options.terminal == nullptr ? &std::cout : options.terminal) {
        if (options.register_builtins) {
            register_builtin_workers(workers);
        }
    }
};

Engine::Engine(EngineOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {
}

Engine::~Engine() = default;
Engine::Engine(Engine &&) noexcept = default;
Engine &Engine::operator=(Engine &&) noexcept = default;

WorkerRegistry &Engine::workers() noexcept {
    return impl_->workers;
}

RunResult Engine::run(const std::filesystem::path &scenario) const {
    return Coordinator(impl_->workers,
        impl_->queues,
        impl_->statistics_root,
        *impl_->terminal)
        .run(scenario);
}

SweepResult Engine::run_sweep(const std::filesystem::path &scenario) const {
    return Coordinator(impl_->workers,
        impl_->queues,
        impl_->statistics_root,
        *impl_->terminal)
        .run_sweep(scenario);
}

} // namespace dataflow
