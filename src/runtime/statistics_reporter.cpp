#include <dataflow/runtime/statistics_reporter.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace dataflow {
namespace {

std::string csv_escape(std::string_view value) {
    if (value.find_first_of(",\"\r\n") == std::string_view::npos) {
        return std::string(value);
    }
    std::string result{"\""};
    for (const auto ch : value) {
        result += ch == '\"' ? "\"\"" : std::string(1, ch);
    }
    result += '\"';
    return result;
}

} // namespace

std::size_t StatisticsReporter::write_terminal(std::ostream &output,
    const ConfigGraph::TerminalReportSpec &terminal,
    std::span<const StatisticsReportEntry> providers,
    bool final_report,
    bool merged,
    std::string_view context,
    std::size_t replace_lines) const {
    if (!terminal.enabled) {
        return 0;
    }

    std::ostringstream report;
    struct Row {
        std::string worker;
        std::string metric;
        std::string value;
        std::string state;
    };
    std::vector<Row> rows;
    std::size_t worker_width = 6;
    std::size_t metric_width = 6;
    std::size_t value_width = 5;
    for (const auto &entry : providers) {
        const auto state = entry.provider->criterion_satisfied()
                               ? std::string("ready")
                               : std::string("collecting");
        for (auto statistic : entry.provider->statistics()) {
            worker_width = std::max(worker_width, entry.node_id.size());
            metric_width = std::max(metric_width, statistic.metric.size());
            value_width = std::max(value_width, statistic.value.size());
            rows.push_back({entry.node_id,
                std::move(statistic.metric),
                std::move(statistic.value),
                state});
        }
    }

    const auto border = [&]() {
        report << '+' << std::string(worker_width + 2, '-') << '+'
               << std::string(metric_width + 2, '-') << '+'
               << std::string(value_width + 2, '-') << '+'
               << std::string(12, '-') << "+\n";
    };
    report << (final_report ? "Final " : "Live ")
           << (merged ? "merged " : "") << "statistics";
    if (!context.empty()) {
        report << " [" << context << ']';
    }
    report << '\n';
    border();
    report << "| " << std::left << std::setw(worker_width) << "Worker"
           << " | " << std::setw(metric_width) << "Metric"
           << " | " << std::setw(value_width) << "Value"
           << " | " << std::setw(10) << "State"
           << " |\n";
    border();
    for (const auto &row : rows) {
        report << "| " << std::left << std::setw(worker_width) << row.worker
               << " | " << std::setw(metric_width) << row.metric << " | "
               << std::setw(value_width) << row.value << " | "
               << std::setw(10) << row.state << " |\n";
    }
    border();

    const auto rendered = std::move(report).str();
    if (terminal.update_in_place && replace_lines != 0) {
        output << "\x1b[" << replace_lines << "A\r\x1b[J";
    }
    output << rendered;
    output.flush();
    return static_cast<std::size_t>(
        std::ranges::count(rendered, '\n'));
}

void StatisticsReporter::write_csv(const std::filesystem::path &file,
    std::span<const StatisticsReportEntry> providers) const {
    std::ofstream output(file);
    if (!output) {
        throw std::runtime_error(
            "could not open statistics file: " + file.string());
    }
    output << "worker,metric,value,criterion_satisfied,criterion\n";
    for (const auto &entry : providers) {
        const auto satisfied =
            entry.provider->criterion_satisfied() ? "true" : "false";
        const auto criterion =
            csv_escape(entry.provider->criterion_description());
        for (const auto &statistic : entry.provider->statistics()) {
            output << csv_escape(entry.node_id) << ','
                   << csv_escape(statistic.metric) << ','
                   << csv_escape(statistic.value) << ',' << satisfied << ','
                   << criterion << '\n';
        }
    }
}

} // namespace dataflow
