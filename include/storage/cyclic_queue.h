#pragma once

#include <interfaces/iqueue.h>
#include <storage/queue_types.h>

#include <cassert>
#include <complex>
#include <cstddef>
#include <cstring>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dataflow {

struct CyclicQueueParams final {
    std::size_t id_;
    std::size_t capacity_;
    ScalarType type_;
};

template <typename ViewType>
    requires std::is_trivially_copyable_v<ViewType>
struct SegmentedSpan {
    class iterator {
        const SegmentedSpan *owner_ = nullptr;
        std::size_t index_{};

        friend struct SegmentedSpan;

        constexpr iterator(const SegmentedSpan *owner,
            std::size_t index) noexcept
            : owner_(owner), index_(index) {
        }

    public:
        using iterator_category = std::forward_iterator_tag;
        using iterator_concept = std::forward_iterator_tag;
        using value_type = std::remove_cv_t<ViewType>;
        using difference_type = std::ptrdiff_t;
        using reference = ViewType &;
        using pointer = ViewType *;

        constexpr iterator() noexcept = default;

        [[nodiscard]] constexpr reference operator*() const {
            return (*owner_)[index_];
        }

        [[nodiscard]] constexpr pointer operator->() const {
            return std::addressof(**this);
        }

        constexpr iterator &operator++() noexcept {
            ++index_;
            return *this;
        }

        constexpr iterator operator++(int) noexcept {
            auto previous = *this;
            ++*this;
            return previous;
        }

        friend constexpr bool operator==(const iterator &,
            const iterator &) noexcept = default;
    };

    std::span<ViewType> first;
    std::span<ViewType> second;

    SegmentedSpan() = default;

    SegmentedSpan(std::span<ViewType> first_, std::span<ViewType> second_)
        : first(first_), second(second_) {
    }

    [[nodiscard]] constexpr std::size_t size() const noexcept {
        return first.size() + second.size();
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return first.empty() && second.empty();
    }

    [[nodiscard]] constexpr bool is_contiguous() const noexcept {
        return second.empty();
    }

    template <class F> constexpr void for_each(const F &func) const {
        for (auto &value : first) {
            func(value);
        }

        for (auto &value : second) {
            func(value);
        }
    }

    [[nodiscard]] constexpr ViewType &operator[](std::size_t index) const {
        if (index < first.size()) {
            return first[index];
        }

        return second[index - first.size()];
    }

    [[nodiscard]] constexpr iterator begin() const noexcept {
        return iterator(this, 0);
    }

    [[nodiscard]] constexpr iterator end() const noexcept {
        return iterator(this, size());
    }

    std::optional<std::span<ViewType>> as_contiguous_span() const noexcept {
        if (!is_contiguous())
            return std::nullopt;
        return first;
    }

    void copy_into(void *ptr) const {
        std::memcpy(ptr, first.data(), first.size_bytes());
        std::memcpy(static_cast<std::byte *>(ptr) + first.size_bytes(),
            second.data(),
            second.size_bytes());
    }

    void copy_into(std::span<std::remove_cv_t<ViewType>> sp) const {
        copy_into(sp.data());
    }
};

template <typename ElemType>
    requires std::is_trivially_copyable_v<ElemType>
class CyclicQueue;

template <typename ElemType> class ReadTransaction {
public:
    ReadTransaction(const ReadTransaction &) = delete;
    ReadTransaction &operator=(const ReadTransaction &) = delete;

    ReadTransaction(ReadTransaction &&other) noexcept
        : queue_(std::exchange(other.queue_, nullptr)),
          count_(std::exchange(other.count_, 0)) {
    }

    ReadTransaction &operator=(ReadTransaction &&other) noexcept {
        if (this == &other) {
            return *this;
        }

        release();

        queue_ = std::exchange(other.queue_, nullptr);
        count_ = std::exchange(other.count_, 0);

        return *this;
    }

    ~ReadTransaction() {
        release();
    }

    [[nodiscard]] SegmentedSpan<const ElemType> spans() const noexcept {
        if (queue_ == nullptr || count_ == 0) {
            return {};
        }

        return queue_->read(count_);
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return count_;
    }

    [[nodiscard]] bool empty() const noexcept {
        return count_ == 0;
    }

    void consume(std::size_t count) {
        ensure_active();

        if (count > count_) {
            throw std::out_of_range(
                "cannot consume more elements than the transaction holds");
        }

        queue_->consume_impl(count);
        release();
    }

    void consume_all() {
        consume(count_);
    }

    void cancel() noexcept {
        release();
    }

private:
    friend class CyclicQueue<ElemType>;

    ReadTransaction(CyclicQueue<ElemType> &queue, std::size_t count) noexcept
        : queue_(&queue), count_(count) {
    }

    void ensure_active() const {
        if (queue_ == nullptr) {
            throw std::logic_error("read transaction is no longer active");
        }
    }

    void release() noexcept {
        if (queue_ == nullptr) {
            return;
        }

        queue_->read_transaction_active_ = false;
        queue_ = nullptr;
        count_ = 0;
    }

    CyclicQueue<ElemType> *queue_ = nullptr;
    std::size_t count_ = 0;
};

template <typename ElemType> class WriteTransaction {
public:
    WriteTransaction(const WriteTransaction &) = delete;
    WriteTransaction &operator=(const WriteTransaction &) = delete;

    WriteTransaction(WriteTransaction &&other) noexcept
        : queue_(std::exchange(other.queue_, nullptr)),
          count_(std::exchange(other.count_, 0)) {
    }

    WriteTransaction &operator=(WriteTransaction &&other) noexcept {
        if (this == &other) {
            return *this;
        }

        release();

        queue_ = std::exchange(other.queue_, nullptr);
        count_ = std::exchange(other.count_, 0);

        return *this;
    }

    ~WriteTransaction() {
        release();
    }

    [[nodiscard]] SegmentedSpan<ElemType> spans() noexcept {
        if (queue_ == nullptr || count_ == 0) {
            return {};
        }

        return queue_->write_spans(count_);
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return count_;
    }

    [[nodiscard]] bool empty() const noexcept {
        return count_ == 0;
    }

    void commit(std::size_t count) {
        ensure_active();

        if (count > count_) {
            throw std::out_of_range(
                "cannot commit more elements than the transaction reserved");
        }

        queue_->commit_impl(count);
        release();
    }

    void commit_all() {
        commit(count_);
    }

    void cancel() noexcept {
        release();
    }

private:
    friend class CyclicQueue<ElemType>;

    WriteTransaction(CyclicQueue<ElemType> &queue, std::size_t count) noexcept
        : queue_(&queue), count_(count) {
    }

    void ensure_active() const {
        if (queue_ == nullptr) {
            throw std::logic_error("write transaction is no longer active");
        }
    }

    void release() noexcept {
        if (queue_ == nullptr) {
            return;
        }

        queue_->write_transaction_active_ = false;
        queue_ = nullptr;
        count_ = 0;
    }

    CyclicQueue<ElemType> *queue_ = nullptr;
    std::size_t count_ = 0;
};

template <typename ElemType>
    requires std::is_trivially_copyable_v<ElemType>
class CyclicQueue final : public IQueue {
public:
    using const_read_view = SegmentedSpan<const ElemType>;
    using read_view = SegmentedSpan<ElemType>;

private:
    friend class ReadTransaction<ElemType>;
    friend class WriteTransaction<ElemType>;

    std::vector<ElemType> data_;
    std::size_t read_pos_{}, write_pos_{};
    std::size_t size_{};
    std::size_t id_{};

    bool read_transaction_active_ = false;
    bool write_transaction_active_ = false;

    [[nodiscard]] std::size_t advance(std::size_t position,
        std::size_t amount) const noexcept {
        position += amount;

        if (position >= capacity()) {
            position -= capacity();
        }

        return position;
    }

    void consume_impl(std::size_t count) noexcept {
        assert(count <= elems_stored());

        read_pos_ = advance(read_pos_, count);
        size_ -= count;
    }

    void commit_impl(std::size_t count) noexcept {
        assert(count <= elems_free());

        write_pos_ = advance(write_pos_, count);
        size_ += count;
    }

    [[nodiscard]] const_read_view read(std::size_t count) const noexcept {
        assert(count <= elems_stored());

        const auto first_count = std::min(count, capacity() - read_pos_);
        const auto second_count = count - first_count;

        return {
            std::span<const ElemType>{data_.data() + read_pos_, first_count},
            std::span<const ElemType>{data_.data(), second_count}};
    }

    [[nodiscard]] read_view write_spans(std::size_t count) noexcept {
        assert(count <= elems_free());

        const auto first_count = std::min(count, capacity() - write_pos_);
        const auto second_count = count - first_count;

        return {std::span<ElemType>{data_.data() + write_pos_, first_count},
            std::span<ElemType>{data_.data(), second_count}};
    }

public:
    explicit CyclicQueue(const CyclicQueueParams &params)
        : CyclicQueue(params.capacity_, params.id_) {
    }

    CyclicQueue(std::size_t capacity, std::size_t id)
        : data_(capacity), id_(id) {
        if (!capacity)
            throw std::invalid_argument(
                "Capacity must be positive for cyclic queues");
    }

    [[nodiscard]] ScalarType type() const noexcept override {
        return scalar_id_v<ElemType>;
    }

    [[nodiscard]] std::size_t id() const noexcept override {
        return id_;
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return data_.size();
    }

    [[nodiscard]] bool empty() const noexcept {
        return size_ == 0;
    }

    [[nodiscard]] bool full() const noexcept {
        return size_ == capacity();
    }

    [[nodiscard]] std::size_t elems_stored() const noexcept {
        return size_;
    }

    [[nodiscard]] std::size_t elems_free() const noexcept {
        return capacity() - elems_stored();
    }

    [[nodiscard]] ReadTransaction<ElemType> begin_read(std::size_t max_count) {
        if (read_transaction_active_) {
            throw std::logic_error(
                "CyclicQueue already has an active read transaction");
        }

        const auto count = std::min(max_count, elems_stored());
        read_transaction_active_ = true;

        return ReadTransaction<ElemType>{*this, count};
    }

    [[nodiscard]] WriteTransaction<ElemType> begin_write(
        std::size_t max_count) {
        if (write_transaction_active_) {
            throw std::logic_error(
                "CyclicQueue already has an active write transaction");
        }

        const auto count = std::min(max_count, elems_free());
        write_transaction_active_ = true;

        return WriteTransaction<ElemType>{*this, count};
    }

    [[nodiscard]] ReadTransaction<ElemType> begin_read_exact(
        std::size_t count) {
        if (elems_stored() < count) {
            throw std::length_error(
                "CyclicQueue does not contain enough readable elements");
        }

        return begin_read(count);
    }

    [[nodiscard]] WriteTransaction<ElemType> begin_write_exact(
        std::size_t count) {
        if (elems_free() < count) {
            throw std::length_error(
                "CyclicQueue does not have enough writable capacity");
        }

        return begin_write(count);
    }
};

class CyclicQueueFactory final {
    using Creator =
        std::function<std::unique_ptr<IQueue>(const CyclicQueueParams &)>;
    std::unordered_map<ScalarType, Creator> creators_;

public:
    CyclicQueueFactory() {
        register_type_list(SupportedScalarTypes{});
    }

    template <typename ElemType> void register_type(ScalarType id) {
        creators_.emplace(id,
            [](const CyclicQueueParams &params) -> std::unique_ptr<IQueue> {
                return std::make_unique<CyclicQueue<ElemType>>(params);
            });
    }

    template <typename... Ts> void register_types() {
        (register_type<Ts>(scalar_id_v<Ts>), ...);
    }

    template <typename... Ts> void register_type_list(ScalarTypeList<Ts...>) {
        register_types<Ts...>();
    }

    [[nodiscard]] std::unique_ptr<IQueue> create(
        const CyclicQueueParams &params) const {
        auto it = creators_.find(params.type_);
        if (it != creators_.end()) {
            return it->second(params);
        }
        return nullptr;
    }
};

} // namespace dataflow
