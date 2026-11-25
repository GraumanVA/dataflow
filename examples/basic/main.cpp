#include "basic_workers.h"

#include <dataflow/engine.h>
#include <dataflow/graph_error.h>

#include <exception>
#include <iostream>

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: dataflow_basic_example <scenario.yaml>\n";
        return 2;
    }
    try {
        dataflow::Engine engine;
        example::register_workers(engine.workers());
        const auto result = engine.run(argv[1]);
        std::cout << "Simulation stopped: "
                  << dataflow::to_string(result.termination) << '\n';
        return 0;
    } catch (const dataflow::GraphCompilationError &error) {
        std::cerr << error.what() << '\n';
        for (const auto &diagnostic : error.diagnostics()) {
            std::cerr << "  - " << diagnostic.message << '\n';
        }
        return 1;
    }
}
