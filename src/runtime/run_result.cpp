#include <dataflow/run_result.h>

namespace dataflow {

std::string_view to_string(RunTermination termination) noexcept {
    switch (termination) {
    case RunTermination::statistics_satisfied:
        return "statistics satisfied";
    case RunTermination::quiescent:
        return "graph quiescent";
    case RunTermination::quiescent_before_statistics:
        return "quiescent before statistics were satisfied";
    }
    return "unknown";
}

} // namespace dataflow
