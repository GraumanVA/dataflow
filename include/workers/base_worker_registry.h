#pragma once

#include <workers/builtin_worker_registry.h>

[[deprecated("use dataflow::register_builtin_workers")]] inline void
register_base_workers(dataflow::WorkerRegistry &workers) {
    dataflow::register_builtin_workers(workers);
}
