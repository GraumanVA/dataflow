#include <workers/ber_observer.h>
#include <workers/broadcaster.h>
#include <workers/builtin_worker_registry.h>

namespace dataflow {

void register_builtin_workers(WorkerRegistry &workers) {
    workers.register_worker<Broadcaster<std::uint8_t>>("broadcast_u8");
    workers.register_worker<Broadcaster<std::int32_t>>("broadcast_i32");
    workers.register_observer<BerObserver<std::uint8_t>>("ber_u8");
    workers.register_observer<BerObserver<std::int32_t>>("ber_i32");
}

} // namespace dataflow
