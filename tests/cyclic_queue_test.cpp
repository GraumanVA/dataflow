#include <storage/cyclic_queue.h>

#include <cstdint>
#include <iterator>
#include <stdexcept>

namespace {

static_assert(
    std::forward_iterator<dataflow::SegmentedSpan<std::int32_t>::iterator>);

void require(bool condition, const char *message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

} // namespace

int main() {
    dataflow::CyclicQueue<std::int32_t> queue(4, 7);
    {
        auto write = queue.begin_write_exact(3);
        std::int32_t value = 1;
        for (auto &element : write.spans()) {
            element = value++;
        }
        write.commit_all();
    }
    {
        auto read = queue.begin_read_exact(2);
        require(read.spans()[0] == 1 && read.spans()[1] == 2,
            "initial queue order was wrong");
        read.consume_all();
    }
    {
        auto write = queue.begin_write_exact(3);
        std::int32_t value = 4;
        for (auto &element : write.spans()) {
            element = value++;
        }
        require(!write.spans().is_contiguous(),
            "wrapped write did not expose segmented storage");
        write.commit_all();
    }
    {
        auto read = queue.begin_read_exact(4);
        std::int32_t expected = 3;
        for (const auto value : read.spans()) {
            require(value == expected, "wrapped queue order was wrong");
            ++expected;
        }
        require(expected == 7, "segmented span iterator had the wrong length");
        read.consume_all();
    }

    {
        auto cancelled = queue.begin_write_exact(2);
        cancelled.spans()[0] = 99;
    }
    require(queue.empty(), "cancelled write committed elements");
    auto replacement = queue.begin_write_exact(1);
    replacement.spans()[0] = 7;
    replacement.commit_all();
}
