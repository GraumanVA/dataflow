#pragma once

#include <dataflow/interfaces/worker_ports.h>
#include <dataflow/storage/cyclic_queue.h>
#include <dataflow/worker_context.h>

#include <exception>
#include <format>
#include <memory>
#include <span>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace dataflow {

struct WorkerBindings {
    std::vector<std::shared_ptr<IQueue>> inputs;
    std::vector<std::shared_ptr<IQueue>> outputs;
    std::shared_ptr<const WorkerPorts> ports;
};

class IWorker {
public:
    [[nodiscard]] virtual bool is_ready() const = 0;
    virtual bool process_one() = 0;

    bool process() {
        bool progress_made = false;
        while (process_one()) {
            progress_made = true;
        }
        return progress_made;
    }

    // Called once on each execution instance before queues are bound.
    virtual void initialize_random_streams(const WorkerContext &) {}
    virtual void bind(WorkerBindings bindings) = 0;

    virtual ~IWorker() = default;

    IWorker() = default;

    IWorker(const IWorker &) = delete;
    IWorker &operator=(const IWorker &) = delete;

    IWorker(IWorker &&) = delete;
    IWorker &operator=(IWorker &&) = delete;
};

template <typename... Types> struct InputPorts {};

template <typename... Types> struct OutputPorts {};

template <typename Inputs, typename Outputs> class TypedWorker;

template <typename... InTypes, typename... OutTypes>
class TypedWorker<InputPorts<InTypes...>, OutputPorts<OutTypes...>>
    : public IWorker {
    static_assert((std::is_trivially_copyable_v<InTypes> && ...));
    static_assert((std::is_trivially_copyable_v<OutTypes> && ...));

    using input_queues = std::tuple<std::shared_ptr<CyclicQueue<InTypes>>...>;
    using output_queues = std::tuple<std::shared_ptr<CyclicQueue<OutTypes>>...>;
    using input_transactions = std::tuple<ReadTransaction<InTypes>...>;
    using output_transactions = std::tuple<WriteTransaction<OutTypes>...>;

    input_queues inputs_;
    output_queues outputs_;
    std::shared_ptr<const WorkerPorts> ports_;
    bool bound_ = false;

    template <typename T>
    [[nodiscard]] static std::shared_ptr<CyclicQueue<T>> cast_queue(
        const std::shared_ptr<IQueue> &queue) {
        auto result = std::dynamic_pointer_cast<CyclicQueue<T>>(queue);
        if (!result) {
            throw std::invalid_argument(
                "TypedWorker queue has the wrong dynamic type");
        }
        return result;
    }

    template <std::size_t... I>
    [[nodiscard]] static input_queues cast_inputs(
        const std::vector<std::shared_ptr<IQueue>> &queues,
        std::index_sequence<I...>) {
        return input_queues{cast_queue<InTypes>(queues[I])...};
    }

    template <std::size_t... I>
    [[nodiscard]] static output_queues cast_outputs(
        const std::vector<std::shared_ptr<IQueue>> &queues,
        std::index_sequence<I...>) {
        return output_queues{cast_queue<OutTypes>(queues[I])...};
    }

    template <std::size_t... I>
    void validate_input_layout(std::index_sequence<I...>) const {
        if (!((ports_->inputs[I].type == scalar_id_v<InTypes>) && ...)) {
            throw std::invalid_argument(
                "TypedWorker input port type does not match its declaration");
        }
        if (!((ports_->inputs[I].transaction_size > 0) && ...)) {
            throw std::invalid_argument(
                "TypedWorker input transaction sizes must be positive");
        }
    }

    template <std::size_t... I>
    void validate_output_layout(std::index_sequence<I...>) const {
        if (!((ports_->outputs[I].type == scalar_id_v<OutTypes>) && ...)) {
            throw std::invalid_argument(
                "TypedWorker output port type does not match its declaration");
        }
        if (!((ports_->outputs[I].transaction_size > 0) && ...)) {
            throw std::invalid_argument(
                "TypedWorker output transaction sizes must be positive");
        }
    }

    void validate_layout() const {
        if (ports_->inputs.size() != sizeof...(InTypes) ||
            ports_->outputs.size() != sizeof...(OutTypes)) {
            throw std::invalid_argument(
                "TypedWorker port count does not match its declaration");
        }
        validate_input_layout(std::index_sequence_for<InTypes...>{});
        validate_output_layout(std::index_sequence_for<OutTypes...>{});
    }

    template <std::size_t... I>
    void validate_input_capacities(std::index_sequence<I...>) const {
        if (!((std::get<I>(inputs_)->capacity() >=
                  ports_->inputs[I].transaction_size) &&
                ...)) {
            throw std::invalid_argument(
                "TypedWorker input queue is smaller than its transaction");
        }
    }

    template <std::size_t... I>
    void validate_output_capacities(std::index_sequence<I...>) const {
        if (!((std::get<I>(outputs_)->capacity() >=
                  ports_->outputs[I].transaction_size) &&
                ...)) {
            throw std::invalid_argument(
                "TypedWorker output queue is smaller than its transaction");
        }
    }

    template <std::size_t... I>
    [[nodiscard]] bool inputs_ready(std::index_sequence<I...>) const {
        return ((std::get<I>(inputs_)->elems_stored() >=
                    ports_->inputs[I].transaction_size) &&
                ...);
    }

    template <std::size_t... I>
    [[nodiscard]] bool outputs_ready(std::index_sequence<I...>) const {
        return ((std::get<I>(outputs_)->elems_free() >=
                    ports_->outputs[I].transaction_size) &&
                ...);
    }

    template <std::size_t... I>
    [[nodiscard]] input_transactions begin_reads(std::index_sequence<I...>) {
        return input_transactions{std::get<I>(inputs_)->begin_read_exact(
            ports_->inputs[I].transaction_size)...};
    }

    template <std::size_t... I>
    [[nodiscard]] output_transactions begin_writes(std::index_sequence<I...>) {
        return output_transactions{std::get<I>(outputs_)->begin_write_exact(
            ports_->outputs[I].transaction_size)...};
    }

    template <std::size_t... I>
    [[nodiscard]] static auto make_input_views(input_transactions &transactions,
        std::index_sequence<I...>) {
        return input_views{std::get<I>(transactions).spans()...};
    }

    template <std::size_t... I>
    [[nodiscard]] static auto make_output_views(
        output_transactions &transactions,
        std::index_sequence<I...>) {
        return output_views{std::get<I>(transactions).spans()...};
    }

protected:
    using input_views =
        std::tuple<typename CyclicQueue<InTypes>::const_read_view...>;
    using output_views =
        std::tuple<typename CyclicQueue<OutTypes>::read_view...>;

    [[nodiscard]] const WorkerPorts &worker_ports() const {
        if (!ports_) {
            throw std::logic_error("TypedWorker has no bound port layout");
        }
        return *ports_;
    }

    TypedWorker() = default;

    explicit TypedWorker(std::shared_ptr<const WorkerPorts> ports)
        : ports_(std::move(ports)) {
        if (!ports_) {
            throw std::invalid_argument("TypedWorker requires a port layout");
        }
        validate_layout();
    }

    virtual void work_one(input_views &inputs, output_views &outputs) = 0;

public:
    void bind(WorkerBindings bindings) override {
        bound_ = false;
        if (!ports_) {
            ports_ = std::move(bindings.ports);
        }
        if (!ports_) {
            throw std::invalid_argument(
                "TypedWorker requires a prepared port layout when bound");
        }
        validate_layout();
        if (bindings.inputs.size() != sizeof...(InTypes) ||
            bindings.outputs.size() != sizeof...(OutTypes)) {
            throw std::invalid_argument(
                "TypedWorker bindings do not match its declared ports");
        }

        auto inputs =
            cast_inputs(bindings.inputs, std::index_sequence_for<InTypes...>{});
        auto outputs = cast_outputs(bindings.outputs,
            std::index_sequence_for<OutTypes...>{});
        inputs_ = std::move(inputs);
        outputs_ = std::move(outputs);
        validate_input_capacities(std::index_sequence_for<InTypes...>{});
        validate_output_capacities(std::index_sequence_for<OutTypes...>{});
        bound_ = true;
    }

    [[nodiscard]] bool is_ready() const override {
        return bound_ && inputs_ready(std::index_sequence_for<InTypes...>{}) &&
               outputs_ready(std::index_sequence_for<OutTypes...>{});
    }

    bool process_one() override {
        if (!is_ready()) {
            return false;
        }

        auto reads = begin_reads(std::index_sequence_for<InTypes...>{});
        auto writes = begin_writes(std::index_sequence_for<OutTypes...>{});
        auto inputs =
            make_input_views(reads, std::index_sequence_for<InTypes...>{});
        auto outputs =
            make_output_views(writes, std::index_sequence_for<OutTypes...>{});

        work_one(inputs, outputs);

        std::apply(
            [](auto &...transaction) { (transaction.consume_all(), ...); },
            reads);
        std::apply(
            [](auto &...transaction) { (transaction.commit_all(), ...); },
            writes);
        return true;
    }
};

template <typename InType, typename OutType>
    requires std::is_trivially_copyable_v<InType> &&
             std::is_trivially_copyable_v<OutType>
class SizedTypedWorker : public IWorker {
private:
    std::vector<std::shared_ptr<CyclicQueue<InType>>> in_queues_;
    std::vector<std::shared_ptr<CyclicQueue<OutType>>> out_queues_;
    std::shared_ptr<const WorkerPorts> ports_;
    std::vector<ReadTransaction<InType>> input_transactions_;
    std::vector<WriteTransaction<OutType>> output_transactions_;
    std::vector<typename CyclicQueue<InType>::const_read_view> input_views_;
    std::vector<typename CyclicQueue<OutType>::read_view> output_views_;
    bool bound_ = false;

    void clear_workspace() noexcept {
        input_views_.clear();
        output_views_.clear();
        input_transactions_.clear();
        output_transactions_.clear();
    }

    void validate() const {
        if (in_queues_.size() != ports_->inputs.size())
            throw std::invalid_argument(
                "SizedTypedWorker: wrong number of input queues or reqs");

        if (out_queues_.size() != ports_->outputs.size())
            throw std::invalid_argument(
                "SizedTypedWorker: wrong number of output queues or reqs");

        for (std::size_t i{}; i < in_queues_.size(); ++i) {
            if (ports_->inputs[i].type != scalar_id_v<InType>) {
                throw std::invalid_argument(
                    "SizedTypedWorker: input port has the wrong scalar type");
            }
            if (in_queues_[i]->capacity() <
                ports_->inputs[i].transaction_size) {
                const std::string msg = std::format(
                    "SizedTypedWorker: input queue number {} has capacity "
                    "{}, but the requirement is {}",
                    i,
                    in_queues_[i]->capacity(),
                    ports_->inputs[i].transaction_size);
                throw std::invalid_argument(msg);
            }
        }

        for (std::size_t i{}; i < out_queues_.size(); ++i) {
            if (ports_->outputs[i].type != scalar_id_v<OutType>) {
                throw std::invalid_argument(
                    "SizedTypedWorker: output port has the wrong scalar type");
            }
            if (out_queues_[i]->capacity() <
                ports_->outputs[i].transaction_size) {
                const std::string msg = std::format(
                    "SizedTypedWorker: output queue number {} has capacity "
                    "{}, but the requirement is {}",
                    i,
                    out_queues_[i]->capacity(),
                    ports_->outputs[i].transaction_size);
                throw std::invalid_argument(msg);
            }
        }
    }

    template <typename T>
    [[nodiscard]] static std::vector<std::shared_ptr<CyclicQueue<T>>>
    cast_queues(const std::vector<std::shared_ptr<IQueue>> &queues) {
        std::vector<std::shared_ptr<CyclicQueue<T>>> result;
        result.reserve(queues.size());
        for (const auto &q : queues) {
            auto dyn_ptr = std::dynamic_pointer_cast<CyclicQueue<T>>(q);
            if (!dyn_ptr)
                throw std::invalid_argument(
                    "The IQueues must have CyclicQueue<T> dynamic type");
            result.push_back(std::move(dyn_ptr));
        }
        return result;
    }

public:
    using input_view = typename CyclicQueue<InType>::const_read_view;
    using output_view = typename CyclicQueue<OutType>::read_view;

protected:
    [[nodiscard]] const WorkerPorts &worker_ports() const {
        if (!ports_) {
            throw std::logic_error("SizedTypedWorker has no bound port layout");
        }
        return *ports_;
    }

    SizedTypedWorker() = default;

public:
    explicit SizedTypedWorker(std::shared_ptr<const WorkerPorts> ports)
        : ports_(std::move(ports)) {
        if (!ports_) {
            throw std::invalid_argument(
                "SizedTypedWorker requires a port layout");
        }
    }

    [[nodiscard]] bool is_ready() const override {
        if (!bound_) {
            return false;
        }

        for (std::size_t i{}; i < in_queues_.size(); ++i) {
            if (in_queues_[i]->elems_stored() <
                ports_->inputs[i].transaction_size)
                return false;
        }

        for (std::size_t i{}; i < out_queues_.size(); ++i) {
            if (out_queues_[i]->elems_free() <
                ports_->outputs[i].transaction_size)
                return false;
        }

        return true;
    }

    bool process_one() override {
        if (!is_ready()) {
            return false;
        }

        clear_workspace();
        try {
            for (std::size_t i{}; i < in_queues_.size(); ++i) {
                input_transactions_.push_back(in_queues_[i]->begin_read_exact(
                    ports_->inputs[i].transaction_size));
                input_views_.push_back(input_transactions_.back().spans());
            }

            for (std::size_t i{}; i < out_queues_.size(); ++i) {
                output_transactions_.push_back(
                    out_queues_[i]->begin_write_exact(
                        ports_->outputs[i].transaction_size));
                output_views_.push_back(output_transactions_.back().spans());
            }

            work_one(input_views_, output_views_);

            for (auto &transaction : input_transactions_) {
                transaction.consume_all();
            }
            for (auto &transaction : output_transactions_) {
                transaction.commit_all();
            }
        } catch (...) {
            clear_workspace();
            throw;
        }

        clear_workspace();
        return true;
    }

    virtual void work_one(
        std::span<typename CyclicQueue<InType>::const_read_view> inputs,
        std::span<typename CyclicQueue<OutType>::read_view> outputs) = 0;

    void bind(WorkerBindings bindings) override {
        bound_ = false;
        if (!ports_) {
            ports_ = std::move(bindings.ports);
        }
        if (!ports_) {
            throw std::invalid_argument(
                "SizedTypedWorker requires a prepared port layout when bound");
        }
        auto inputs = cast_queues<InType>(bindings.inputs);
        auto outputs = cast_queues<OutType>(bindings.outputs);

        in_queues_ = std::move(inputs);
        out_queues_ = std::move(outputs);
        validate();
        clear_workspace();
        input_transactions_.reserve(in_queues_.size());
        output_transactions_.reserve(out_queues_.size());
        input_views_.reserve(in_queues_.size());
        output_views_.reserve(out_queues_.size());
        bound_ = true;
    }
};

struct WorkerFacadeTag {};

template <class ConfigType, typename InType, typename OutType>
    requires std::is_trivially_copyable_v<InType> &&
                 std::is_trivially_copyable_v<OutType>
class WorkerFacade : public SizedTypedWorker<InType, OutType>,
                     public WorkerFacadeTag {
    using Base = SizedTypedWorker<InType, OutType>;

protected:
    WorkerFacade() = default;

    [[nodiscard]] virtual bool can_process() const noexcept {
        return true;
    }

public:
    using Config = ConfigType;
    using typename Base::input_view;
    using typename Base::output_view;

    [[nodiscard]] bool is_ready() const final {
        return Base::is_ready() && can_process();
    }
};

template <class ConfigType, class Inputs, class Outputs>
class TypedWorkerFacade : public TypedWorker<Inputs, Outputs>,
                          public WorkerFacadeTag {
    using Base = TypedWorker<Inputs, Outputs>;

protected:
    TypedWorkerFacade() = default;

    [[nodiscard]] virtual bool can_process() const noexcept {
        return true;
    }

public:
    using Config = ConfigType;
    using typename Base::input_views;
    using typename Base::output_views;

    [[nodiscard]] bool is_ready() const final {
        return Base::is_ready() && can_process();
    }
};

template <class ConfigType, typename OutputType>
using SourceFacade = WorkerFacade<ConfigType, OutputType, OutputType>;

template <class ConfigType, typename InputType>
using SinkFacade = WorkerFacade<ConfigType, InputType, InputType>;

} // namespace dataflow
