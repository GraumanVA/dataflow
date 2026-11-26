#include <dataflow/storage/cyclic_queue.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <iterator>

namespace {

static_assert(
    std::forward_iterator<dataflow::SegmentedSpan<std::int32_t>::iterator>);

TEST(CyclicQueue, PreservesOrderAcrossWraparound) {
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
        EXPECT_EQ(read.spans()[0], 1);
        EXPECT_EQ(read.spans()[1], 2);
        read.consume_all();
    }
    {
        auto write = queue.begin_write_exact(3);
        std::int32_t value = 4;
        for (auto &element : write.spans()) {
            element = value++;
        }
        EXPECT_FALSE(write.spans().is_contiguous());
        write.commit_all();
    }
    {
        auto read = queue.begin_read_exact(4);
        std::int32_t expected = 3;
        for (const auto value : read.spans()) {
            EXPECT_EQ(value, expected);
            ++expected;
        }
        EXPECT_EQ(expected, 7);
        read.consume_all();
    }
}

TEST(CyclicQueue, CanWriteAfterCancellingATransaction) {
    dataflow::CyclicQueue<std::int32_t> queue(4, 7);
    {
        auto cancelled = queue.begin_write_exact(2);
        cancelled.spans()[0] = 99;
    }
    ASSERT_TRUE(queue.empty());
    auto replacement = queue.begin_write_exact(1);
    replacement.spans()[0] = 7;
    replacement.commit_all();
}

} // namespace
