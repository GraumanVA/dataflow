#include <dataflow/engine.h>

#include <cstdint>
#include <exception>
#include <iostream>

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: dataflow <scenario.yaml>\n";
        return 2;
    }

    try {
        const dataflow::Engine engine;
        const auto sweep = engine.run_sweep(argv[1]);
        for (const auto &point : sweep.points) {
            if (sweep.points.size() != 1 || !point.parameters.empty()) {
                std::cout << "Sweep case " << point.index;
                for (const auto &parameter : point.parameters) {
                    const auto &name = parameter.display_name.empty()
                                           ? parameter.name
                                           : parameter.display_name;
                    std::cout << ' ' << name << '='
                              << parameter.value;
                }
                std::cout << '\n';
            }
            std::cout << "Simulation stopped: "
                      << dataflow::to_string(point.run.termination) << '\n'
                      << "Statistics: " << point.run.statistics_file.string()
                      << '\n';
        }
        return 0;
    } catch (const dataflow::GraphCompilationError &error) {
        std::cerr << error.what() << '\n';
        for (const auto &diagnostic : error.diagnostics()) {
            std::cerr << "  - " << diagnostic.message << '\n';
        }
    } catch (const std::exception &error) {
        std::cerr << "dataflow: " << error.what() << '\n';
    }
    return 1;
}
