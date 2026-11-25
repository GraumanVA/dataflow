#pragma once

#include <cstddef>
#include <filesystem>
#include <string_view>

namespace dataflow {

enum class RunTermination {
    statistics_satisfied,
    quiescent,
    quiescent_before_statistics
};

struct RunResult {
    RunTermination termination;
    std::size_t sweeps;
    std::filesystem::path statistics_file;
};

[[nodiscard]] std::string_view to_string(RunTermination termination) noexcept;

} // namespace dataflow
