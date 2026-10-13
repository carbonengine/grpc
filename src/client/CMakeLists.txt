# Copyright © 2025 CCP ehf.
find_package(carbon-prometheus CONFIG REQUIRED)
find_package(Python3 COMPONENTS Development Interpreter REQUIRED)
find_package(protobuf CONFIG REQUIRED)
find_package(grpc CONFIG REQUIRED)
find_package(carbon-core CONFIG REQUIRED)

ccp_add_library(client STATIC
    cached_metrics.cpp
    connection.cpp
    grpc_log.cpp
    jobs.cpp
    metadata.cpp
    resolver.cpp
    stream_counter.cpp
    stream_status_log.cpp
)

set_property(TARGET client PROPERTY CXX_STANDARD 17)
get_target_property(PROMETHEUS_INTERFACE PrometheusModule INTERFACE_INCLUDE_DIRECTORIES)

target_include_directories(client
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:/include>
    PRIVATE
        ${PROMETHEUS_INTERFACE}
)

target_link_libraries(client
    PUBLIC
        protobuf::pyext
        gRPC::grpc++
        CcpCore
)

set_target_properties(client PROPERTIES
        OUTPUT_NAME "carbon_grpc_client"
)
