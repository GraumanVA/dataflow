#pragma once

#include <stdexcept>
#include <string>
#include <vector>

namespace dataflow {

struct GraphDiagnostic {
    std::string message;
};

class GraphCompilationError final : public std::runtime_error {
    std::vector<GraphDiagnostic> diagnostics_;

public:
    explicit GraphCompilationError(std::vector<GraphDiagnostic> diagnostics);

    [[nodiscard]] const std::vector<GraphDiagnostic> &diagnostics()
        const noexcept {
        return diagnostics_;
    }
};

} // namespace dataflow
