#include <dataflow/runtime/coordinator.h>

#include <dataflow/graphs/execution_graph.h>
#include <dataflow/interfaces/statistics.h>
#include <dataflow/interfaces/stop_token.h>
#include <dataflow/runtime/replica_executor.h>
#include <dataflow/runtime/statistics_registry.h>
#include <dataflow/runtime/statistics_reporter.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <format>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dataflow {
namespace {

struct AggregateProvider {
    std::string node_id;
    std::unique_ptr<IStatisticsProvider> aggregate;
};

[[nodiscard]] bool same_port(const PortDescriptor &left,
    const PortDescriptor &right) {
    return left.name == right.name && left.type == right.type &&
           left.transaction_size == right.transaction_size;
}

[[nodiscard]] bool same_ports(const WorkerPorts &left,
    const WorkerPorts &right) {
    return left.inputs.size() == right.inputs.size() &&
           left.outputs.size() == right.outputs.size() &&
           std::ranges::equal(left.inputs, right.inputs, same_port) &&
           std::ranges::equal(left.outputs, right.outputs, same_port);
}

void validate_replica_plan(const ExecutionPlan &expected,
    const ExecutionPlan &candidate) {
    if (expected.workers.size() != candidate.workers.size() ||
        expected.queues.size() != candidate.queues.size() ||
        expected.topological_order != candidate.topological_order) {
        throw std::invalid_argument(
            "replica-derived parameters changed the graph layout");
    }
    for (std::size_t worker{}; worker < expected.workers.size(); ++worker) {
        const auto &left = expected.workers[worker];
        const auto &right = candidate.workers[worker];
        if (left.id != right.id || left.config_node != right.config_node ||
            left.inputs != right.inputs || left.outputs != right.outputs ||
            !same_ports(*left.prepared.ports, *right.prepared.ports)) {
            throw std::invalid_argument(
                "replica-derived parameters changed a worker port layout");
        }
    }
    for (std::size_t queue{}; queue < expected.queues.size(); ++queue) {
        const auto &left = expected.queues[queue];
        const auto &right = candidate.queues[queue];
        if (left.id != right.id || left.type != right.type ||
            left.capacity != right.capacity ||
            left.producer != right.producer ||
            left.producer_port != right.producer_port ||
            left.consumer != right.consumer ||
            left.consumer_port != right.consumer_port) {
            throw std::invalid_argument(
                "replica-derived parameters changed a queue layout");
        }
    }
}

[[nodiscard]] std::vector<SweepParameterValue> public_values(
    const std::vector<SweepNamedValue> &values) {
    std::vector<SweepParameterValue> result;
    result.reserve(values.size());
    for (const auto &value : values) {
        result.push_back(
            {value.name, YAML::Dump(value.value), value.display_name});
    }
    return result;
}

[[nodiscard]] std::string sweep_context(const ExpandedSweepPoint &point) {
    auto result = std::format("Sweep case {}", point.index);
    for (std::size_t index{}; index < point.parameters.size(); ++index) {
        const auto &parameter = point.parameters[index];
        result += index == 0 ? ": " : ", ";
        result += parameter.display_name;
        result += '=';
        result += YAML::Dump(parameter.value);
    }
    return result;
}

void validate_scenario(std::string_view scenario) {
    const std::filesystem::path path(scenario);
    if (scenario.empty() || path.has_root_path() || path.has_parent_path() ||
        path.filename() == "." || path.filename() == "..") {
        throw std::invalid_argument(
            "scenario must be a single safe folder name");
    }
}

std::filesystem::path next_statistics_file(
    const std::filesystem::path &directory) {
    auto candidate = directory / "stats.csv";
    for (std::size_t sequence = 2; std::filesystem::exists(candidate);
        ++sequence) {
        candidate = directory / ("stats_" + std::to_string(sequence) + ".csv");
    }
    return candidate;
}

std::vector<AggregateProvider> discover_aggregates(const ConfigGraph &config,
    const std::vector<std::unique_ptr<ExecutionGraph>> &graphs) {
    std::vector<AggregateProvider> providers;
    if (graphs.empty()) {
        return providers;
    }

    for (std::size_t worker{}; worker < graphs.front()->workers().size();
        ++worker) {
        const auto *first = dynamic_cast<const IStatisticsProvider *>(
            graphs.front()->workers()[worker].get());
        if (first == nullptr) {
            continue;
        }

        for (const auto &graph : graphs) {
            const auto *replica = dynamic_cast<const IStatisticsProvider *>(
                graph->workers()[worker].get());
            if (replica == nullptr) {
                throw std::logic_error(
                    "statistics providers differ between graph replicas");
            }
        }
        providers.push_back({config.node(worker).id, first->make_empty()});
    }
    return providers;
}

const IStatisticsProvider &find_published(
    const GraphStatisticsSnapshot &snapshot,
    const std::string &observer_id) {
    const auto found = std::ranges::find(snapshot.observers,
        observer_id,
        &PublishedStatistic::observer_id);
    if (found == snapshot.observers.end()) {
        throw std::logic_error(
            "statistics observer missing from graph snapshot: " + observer_id);
    }
    return *found->provider;
}

void refresh_aggregates(std::vector<AggregateProvider> &providers,
    const std::vector<GraphStatisticsSnapshot> &snapshots) {
    if (snapshots.empty()) {
        return;
    }
    for (auto &provider : providers) {
        const auto &first = find_published(snapshots.front(), provider.node_id);
        provider.aggregate = first.make_empty();
        for (const auto &snapshot : snapshots) {
            provider.aggregate->merge_from(
                find_published(snapshot, provider.node_id));
        }
    }
}

std::vector<std::size_t> validate_stop_observers(const ConfigGraph &config,
    const std::vector<AggregateProvider> &providers) {
    std::vector<std::size_t> result;
    std::unordered_set<std::string> unique;
    for (const auto &requested : config.runtime().stop.observers) {
        if (!unique.insert(requested).second) {
            throw std::invalid_argument(
                "duplicate stop observer: " + requested);
        }
        const auto found = std::ranges::find(providers,
            requested,
            &AggregateProvider::node_id);
        if (found == providers.end() ||
            !found->aggregate->has_completion_criterion()) {
            throw std::invalid_argument(
                "stop observer has no completion criterion: " + requested);
        }
        result.push_back(static_cast<std::size_t>(found - providers.begin()));
    }
    return result;
}

void update_stop(const std::vector<AggregateProvider> &providers,
    const std::vector<std::size_t> &stop_providers,
    StopController &stop) {
    for (std::size_t criterion{}; criterion < stop_providers.size();
        ++criterion) {
        stop.update(criterion,
            providers[stop_providers[criterion]]
                .aggregate->criterion_satisfied());
    }
}

[[nodiscard]] std::size_t write_table(std::ostream &output,
    const ConfigGraph::TerminalReportSpec &terminal,
    const std::vector<AggregateProvider> &providers,
    bool final_report,
    std::string_view context,
    std::size_t replace_lines) {
    std::vector<StatisticsReportEntry> entries;
    entries.reserve(providers.size());
    for (const auto &provider : providers) {
        entries.push_back({provider.node_id, provider.aggregate.get()});
    }
    return StatisticsReporter{}.write_terminal(output,
        terminal,
        entries,
        final_report,
        true,
        context,
        replace_lines);
}

void write_csv(const std::filesystem::path &file,
    const std::vector<AggregateProvider> &providers) {
    std::vector<StatisticsReportEntry> entries;
    entries.reserve(providers.size());
    for (const auto &provider : providers) {
        entries.push_back({provider.node_id, provider.aggregate.get()});
    }
    StatisticsReporter{}.write_csv(file, entries);
}

[[nodiscard]] std::vector<std::unique_ptr<ExecutionGraph>>
create_replica_graphs(
    const ConfigGraph &config,
    const std::vector<ConfigGraph> &replica_configs,
    const IWorkerFactory &worker_factory,
    const CyclicQueueFactory &queue_factory,
    std::size_t sweep_point) {
    const auto replica_count = config.runtime().threads;
    if (!replica_configs.empty() && replica_configs.size() != replica_count) {
        throw std::invalid_argument(
            "replica configuration count does not match runtime threads");
    }

    std::vector<std::unique_ptr<ExecutionGraph>> graphs;
    graphs.reserve(replica_count);
    if (replica_configs.empty()) {
        const auto plan = GraphCompiler{}.compile(config, worker_factory);
        for (std::size_t replica{}; replica < replica_count; ++replica) {
            graphs.push_back(std::make_unique<ExecutionGraph>(config,
                plan,
                queue_factory,
                sweep_point,
                replica));
        }
    } else {
        std::optional<ExecutionPlan> reference_plan;
        for (std::size_t replica{}; replica < replica_count; ++replica) {
            const auto &replica_config = replica_configs[replica];
            const auto plan =
                GraphCompiler{}.compile(replica_config, worker_factory);
            if (reference_plan) {
                validate_replica_plan(*reference_plan, plan);
            } else {
                reference_plan = plan;
            }
            graphs.push_back(std::make_unique<ExecutionGraph>(replica_config,
                plan,
                queue_factory,
                sweep_point,
                replica));
        }
    }
    return graphs;
}

void run_replica(ExecutionGraph &graph,
    const ConfigGraph &config,
    StatisticsRegistry &registry,
    std::size_t replica,
    std::stop_token token) {
    const auto publication_interval = std::chrono::duration<double>(
        config.runtime().statistics.publish_interval_seconds);
    auto next_publish = std::chrono::steady_clock::now() + publication_interval;
    std::size_t sweeps{};
    const auto publish = [&](bool finished, std::exception_ptr error = {}) {
        registry.publish(collect_graph_statistics(
            replica, sweeps, finished, error, config, graph));
    };

    try {
        publish(false);
        const auto result = ReplicaExecutor{}.run(graph,
            token,
            [&](std::size_t completed_sweeps, bool) {
                sweeps = completed_sweeps;
                const auto now = std::chrono::steady_clock::now();
                if (publication_interval.count() == 0.0 ||
                    now >= next_publish) {
                    publish(false);
                    next_publish = now + publication_interval;
                }
                return false;
            });
        sweeps = result.sweeps;
        publish(true);
    } catch (...) {
        try {
            publish(true, std::current_exception());
        } catch (...) {
            registry.publish(
                {replica, sweeps, true, std::current_exception(), {}});
        }
    }
}

[[nodiscard]] std::vector<std::jthread> launch_replicas(
    const std::vector<std::unique_ptr<ExecutionGraph>> &graphs,
    const ConfigGraph &config,
    StatisticsRegistry &registry) {
    std::vector<std::jthread> threads;
    threads.reserve(graphs.size());
    for (std::size_t replica{}; replica < graphs.size(); ++replica) {
        threads.emplace_back(
            [graph = graphs[replica].get(), &config, &registry, replica](
                std::stop_token token) {
                run_replica(*graph, config, registry, replica, token);
            });
    }
    return threads;
}

void stop_and_join_replicas(std::vector<std::jthread> &threads) {
    for (auto &thread : threads) {
        thread.request_stop();
    }
    for (auto &thread : threads) {
        thread.join();
    }
}

[[nodiscard]] std::exception_ptr replica_error(
    const std::vector<GraphStatisticsSnapshot> &snapshots) {
    const auto failed = std::ranges::find_if(snapshots,
        [](const GraphStatisticsSnapshot &snapshot) {
            return snapshot.error != nullptr;
        });
    return failed != snapshots.end() ? failed->error : nullptr;
}

struct MonitoringResult {
    RunTermination termination = RunTermination::quiescent;
    std::size_t terminal_report_lines{};
    std::uint64_t generation{};
};

[[nodiscard]] MonitoringResult monitor_replicas(const ConfigGraph &config,
    const StatisticsRegistry &registry,
    std::vector<AggregateProvider> &providers,
    const std::vector<std::size_t> &stop_providers,
    std::ostream &output,
    std::string_view report_context,
    std::chrono::steady_clock::time_point report_start) {
    StopController stop(stop_providers.size(), config.runtime().stop.policy);
    const auto &terminal = config.runtime().reporting.terminal;
    const auto terminal_interval =
        std::chrono::duration<double>(terminal.interval_seconds);
    auto next_report = report_start + terminal_interval;
    MonitoringResult result;
    while (true) {
        registry.wait_for_change(result.generation);
        const auto snapshots = registry.collect(result.generation);
        if (snapshots.size() != config.runtime().threads) {
            continue;
        }
        if (replica_error(snapshots)) {
            return result;
        }

        refresh_aggregates(providers, snapshots);
        update_stop(providers, stop_providers, stop);
        const auto now = std::chrono::steady_clock::now();
        if (terminal.enabled &&
            (terminal.interval_seconds == 0.0 || now >= next_report)) {
            result.terminal_report_lines = write_table(output,
                terminal,
                providers,
                false,
                report_context,
                result.terminal_report_lines);
            next_report = now + terminal_interval;
        }

        if (stop.stop_requested()) {
            result.termination = RunTermination::statistics_satisfied;
            return result;
        }
        if (std::ranges::all_of(snapshots,
                [](const GraphStatisticsSnapshot &snapshot) {
                    return snapshot.finished;
                })) {
            result.termination =
                stop_providers.empty()
                    ? RunTermination::quiescent
                    : RunTermination::quiescent_before_statistics;
            return result;
        }
    }
}

[[nodiscard]] RunResult finalize_replicas(const ConfigGraph &config,
    const StatisticsRegistry &registry,
    std::vector<AggregateProvider> &providers,
    const std::filesystem::path &statistics_directory,
    std::ostream &output,
    std::string_view report_context,
    MonitoringResult monitor) {
    const auto snapshots = registry.collect(monitor.generation);
    if (const auto error = replica_error(snapshots)) {
        std::rethrow_exception(error);
    }
    if (snapshots.size() != config.runtime().threads) {
        throw std::logic_error(
            "not every graph replica published final statistics");
    }
    refresh_aggregates(providers, snapshots);
    static_cast<void>(write_table(output,
        config.runtime().reporting.terminal,
        providers,
        true,
        report_context,
        monitor.terminal_report_lines));

    std::filesystem::create_directories(statistics_directory);
    const auto statistics_file = next_statistics_file(statistics_directory);
    write_csv(statistics_file, providers);

    std::size_t sweeps{};
    for (const auto &snapshot : snapshots) {
        sweeps = std::max(sweeps, snapshot.sweeps);
    }
    return {monitor.termination, sweeps, statistics_file};
}

} // namespace

Coordinator::Coordinator(const IWorkerFactory &worker_factory,
    const CyclicQueueFactory &queue_factory,
    std::filesystem::path statistics_root,
    std::ostream &terminal)
    : worker_factory_(worker_factory), queue_factory_(queue_factory),
      statistics_root_(std::move(statistics_root)), terminal_(terminal) {
}

RunResult Coordinator::run(const std::filesystem::path &config_file) const {
    const ConfigGraph config(config_file);
    return run(config);
}

RunResult Coordinator::run(const ConfigGraph &config) const {
    validate_scenario(config.scenario());
    if (config.has_sweep()) {
        throw std::invalid_argument(
            "scenario contains a sweep; use run_sweep instead of run");
    }
    return run_replicas(config, {}, statistics_root_ / config.scenario());
}

SweepResult Coordinator::run_sweep(
    const std::filesystem::path &config_file) const {
    const ConfigGraph config(config_file);
    return run_sweep(config);
}

SweepResult Coordinator::run_sweep(const ConfigGraph &config) const {
    validate_scenario(config.scenario());
    if (!config.has_sweep()) {
        return {{{0, {}, {}, run(config)}}};
    }

    const auto points = config.sweep().expand_points();
    const auto replicas =
        config.sweep().expand_replicas(config.runtime().threads);
    SweepResult result;
    result.points.reserve(points.size());

    for (const auto &point : points) {
        auto point_config = config.with_assignments(point.assignments);
        std::vector<ConfigGraph> replica_configs;
        if (std::ranges::any_of(replicas, [](const auto &replica) {
                return !replica.assignments.empty();
            })) {
            replica_configs.reserve(replicas.size());
            for (const auto &replica : replicas) {
                replica_configs.push_back(
                    point_config.with_assignments(replica.assignments));
            }
        }

        std::vector<SweepReplicaParameters> public_replicas;
        public_replicas.reserve(replicas.size());
        for (const auto &replica : replicas) {
            public_replicas.push_back(
                {replica.replica, public_values(replica.parameters)});
        }

        const auto directory = statistics_root_ / config.scenario() /
                               config.sweep().case_directory_name(point);
        auto run = run_replicas(point_config,
            replica_configs,
            directory,
            sweep_context(point),
            point.index);
        result.points.push_back({point.index,
            public_values(point.parameters),
            std::move(public_replicas),
            std::move(run)});
    }
    return result;
}

RunResult Coordinator::run_replicas(const ConfigGraph &config,
    const std::vector<ConfigGraph> &replica_configs,
    const std::filesystem::path &statistics_directory,
    std::string report_context,
    std::size_t sweep_point) const {
    const auto graphs = create_replica_graphs(
        config, replica_configs, worker_factory_, queue_factory_, sweep_point);
    auto providers = discover_aggregates(config, graphs);
    const auto stop_providers = validate_stop_observers(config, providers);

    const auto report_start = std::chrono::steady_clock::now();
    StatisticsRegistry registry(graphs.size());
    auto threads = launch_replicas(graphs, config, registry);
    const auto result = monitor_replicas(config,
        registry,
        providers,
        stop_providers,
        terminal_,
        report_context,
        report_start);
    stop_and_join_replicas(threads);
    return finalize_replicas(config,
        registry,
        providers,
        statistics_directory,
        terminal_,
        report_context,
        result);
}

} // namespace dataflow
