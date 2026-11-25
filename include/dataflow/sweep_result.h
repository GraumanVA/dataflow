#pragma once

#include <dataflow/run_result.h>

#include <cstddef>
#include <string>
#include <vector>

namespace dataflow {

struct SweepParameterValue {
    std::string name;
    std::string value;
    std::string display_name;
};

struct SweepReplicaParameters {
    std::size_t replica;
    std::vector<SweepParameterValue> parameters;
};

struct SweepPointResult {
    std::size_t index;
    std::vector<SweepParameterValue> parameters;
    std::vector<SweepReplicaParameters> replicas;
    RunResult run;
};

struct SweepResult {
    std::vector<SweepPointResult> points;
};

} // namespace dataflow
