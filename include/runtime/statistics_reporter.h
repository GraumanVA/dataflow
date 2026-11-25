#pragma once

#include <graphs/config_graph.h>
#include <interfaces/statistics.h>

#include <filesystem>
#include <iosfwd>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>

namespace dataflow {

struct StatisticsReportEntry {
    std::string node_id;
    const IStatisticsProvider *provider;
};

class StatisticsReporter {
public:
    [[nodiscard]] std::size_t write_terminal(std::ostream &output,
        const ConfigGraph::TerminalReportSpec &terminal,
        std::span<const StatisticsReportEntry> providers,
        bool final_report,
        bool merged,
        std::string_view context = {},
        std::size_t replace_lines = 0) const;

    void write_csv(const std::filesystem::path &file,
        std::span<const StatisticsReportEntry> providers) const;
};

} // namespace dataflow
