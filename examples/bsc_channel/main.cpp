#include "bsc_workers.h"

#include <dataflow/engine.h>
#include <dataflow/graph_error.h>

#include <exception>
#include <iostream>

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: dataflow_bsc_example <scenario.yaml>\n";
        return 2;
    }

    try {
        std::ios::sync_with_stdio(false);
        std::cin.tie(nullptr);

        dataflow::Engine engine;
        example::bsc::register_workers(engine.workers());
        const auto result = engine.run_sweep(argv[1]);
        for (const auto &point : result.points) {
            std::cout << "Case " << point.index;
            for (const auto &parameter : point.parameters) {
                const auto &name = parameter.display_name.empty()
                                       ? parameter.name
                                       : parameter.display_name;
                std::cout << ", " << name << " = " << parameter.value;
            }
            std::cout << "\n  stopped: "
                      << dataflow::to_string(point.run.termination)
                      << "\n  statistics: "
                      << point.run.statistics_file.string() << '\n';
        }
        return 0;
    } catch (const dataflow::GraphCompilationError &error) {
        std::cerr << error.what() << '\n';
        for (const auto &diagnostic : error.diagnostics()) {
            std::cerr << "  - " << diagnostic.message << '\n';
        }
    } catch (const std::exception &error) {
        std::cerr << "dataflow BSC example: " << error.what() << '\n';
    }
    return 1;
}
