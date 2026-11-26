#pragma once

#include <cstdint>
#include <string>

#include <dataflow/storage/queue_types.h>

namespace dataflow {

class IQueue {
public:
    virtual ~IQueue() = default;

    IQueue() = default;

    IQueue(const IQueue &) = delete;
    IQueue &operator=(const IQueue &) = delete;

    IQueue(IQueue &&) = delete;
    IQueue &operator=(IQueue &&) = delete;

    [[nodiscard]] virtual ScalarType type() const noexcept = 0;
    [[nodiscard]] virtual std::size_t id() const noexcept = 0;
};

} // namespace dataflow
