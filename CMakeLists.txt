cmake_minimum_required(VERSION 3.15)
project(dataflow VERSION 0.1.0 LANGUAGES CXX)

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(DATAFLOW_ENABLE_CLANG_TIDY "Run clang-tidy during builds" OFF)
option(DATAFLOW_BUILD_EXAMPLES "Build dataflow examples" ON)

if(DATAFLOW_ENABLE_CLANG_TIDY)
    find_program(CLANG_TIDY_EXE NAMES "clang-tidy")
    if(CLANG_TIDY_EXE)
        set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_EXE}")
        if(MSVC)
            list(APPEND CMAKE_CXX_CLANG_TIDY --extra-arg-before=--driver-mode=cl)
        endif()
        message(STATUS "clang-tidy found and enabled: ${CLANG_TIDY_EXE}")
    else()
        message(WARNING "clang-tidy executable not found")
    endif()
endif()

set(REFLECTCPP_YAML ON CACHE INTERNAL "")
find_package(reflectcpp REQUIRED)
find_package(yaml-cpp REQUIRED)

add_library(dataflow_core
    src/graphs/config_graph.cpp
    src/graphs/execution_graph.cpp
    src/graphs/execution_plan.cpp
    src/graphs/sweep.cpp
    src/runtime/coordinator.cpp
    src/runtime/engine.cpp
    src/runtime/replica_executor.cpp
    src/runtime/run_result.cpp
    src/runtime/statistics_registry.cpp
    src/runtime/statistics_reporter.cpp
    src/workers/builtin_worker_registry.cpp
)
add_library(dataflow::core ALIAS dataflow_core)
set_target_properties(dataflow_core PROPERTIES EXPORT_NAME core)

target_compile_features(dataflow_core PUBLIC cxx_std_20)
target_include_directories(dataflow_core PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(dataflow_core PUBLIC
    reflectcpp::reflectcpp
    yaml-cpp::yaml-cpp
)

add_executable(dataflow app/main.cpp)
target_link_libraries(dataflow PRIVATE dataflow::core)

if(DATAFLOW_BUILD_EXAMPLES)
    add_executable(dataflow_basic_example examples/basic/main.cpp)
    #target_include_directories(dataflow_basic_example PRIVATE examples)
    target_link_libraries(dataflow_basic_example PRIVATE dataflow::core)

    add_executable(dataflow_bsc_example examples/bsc_channel/main.cpp)
    #target_include_directories(dataflow_bsc_example PRIVATE examples)
    target_link_libraries(dataflow_bsc_example PRIVATE dataflow::core)
endif()

set(MSVC_BASE_OPTIONS /W4 /permissive-)
set(GNU_CLANG_BASE_OPTIONS
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wshadow
)

function(dataflow_enable_warnings target)
    if(MSVC)
        target_compile_options(${target} PRIVATE ${MSVC_BASE_OPTIONS})
    else()
        target_compile_options(${target} PRIVATE ${GNU_CLANG_BASE_OPTIONS})
    endif()
endfunction()

dataflow_enable_warnings(dataflow_core)
dataflow_enable_warnings(dataflow)
if(TARGET dataflow_basic_example)
    dataflow_enable_warnings(dataflow_basic_example)
endif()
if(TARGET dataflow_bsc_example)
    dataflow_enable_warnings(dataflow_bsc_example)
endif()

if(BUILD_TESTING)
    find_package(GTest CONFIG REQUIRED)
    include(GoogleTest)

    foreach(test_target IN ITEMS
            graph_plan_test runtime_feature_test worker_contract_test
            cyclic_queue_test replica_random_test)
        add_executable(${test_target} tests/${test_target}.cpp)
        target_link_libraries(${test_target} PRIVATE dataflow::core GTest::gtest_main)
        dataflow_enable_warnings(${test_target})
        gtest_discover_tests(${test_target})
    endforeach()
endif()

install(TARGETS dataflow_core
    EXPORT dataflowTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(DIRECTORY include/dataflow/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dataflow
    FILES_MATCHING PATTERN "*.h"
    PATTERN "graphs" EXCLUDE
    PATTERN "runtime" EXCLUDE
    PATTERN "workers" EXCLUDE
    PATTERN "stop_token.h" EXCLUDE
)

configure_package_config_file(
    cmake/dataflowConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/dataflowConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/dataflow
)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/dataflowConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/dataflowConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/dataflowConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/dataflow
)
install(EXPORT dataflowTargets
    NAMESPACE dataflow::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/dataflow
)
