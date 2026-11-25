#pragma once

#include <storage/queue_types.h>

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

namespace dataflow {

struct PortDescriptor {
    std::string name;
    ScalarType type;
    std::size_t transaction_size;
};

struct WorkerPorts {
    std::vector<PortDescriptor> inputs;
    std::vector<PortDescriptor> outputs;
};

template <typename T>
[[nodiscard]] inline PortDescriptor port(std::string name,
    std::size_t transaction_size) {
    return {std::move(name), scalar_id_v<T>, transaction_size};
}

} // namespace dataflow
