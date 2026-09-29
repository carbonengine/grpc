# Copyright © 2025 CCP ehf.
find_package(Python3 COMPONENTS Development Interpreter REQUIRED)

ccp_add_library(module STATIC
    connection.cpp
    helpers.cpp
    module.cpp
 )

set_property(TARGET module PROPERTY CXX_STANDARD 17)

target_include_directories(module
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(module
    PRIVATE
        client
    PUBLIC
        Python3::Python
)

set_target_properties(module PROPERTIES
        OUTPUT_NAME "carbon_grpc_module"
)
