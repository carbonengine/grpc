# Copyright © 2026 Fenris Creations ehf.
cmake_minimum_required(VERSION 3.16)
project(logging_sink_test VERSION 1.0.0)

find_package(GTest CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)
find_package(carbon-core CONFIG REQUIRED)

include(GoogleTest)

set(LOGGING_TEST_SRC_FILES
    loggingTest.cpp
)

add_executable(logging_test ${LOGGING_TEST_SRC_FILES})

get_target_property(CARBON_GRPC_INCLUDE_DIR client INTERFACE_INCLUDE_DIRECTORIES)

target_include_directories(logging_test
        PRIVATE
        ${CARBON_GRPC_INCLUDE_DIR}
)


target_link_libraries(logging_test PRIVATE client GTest::gtest GTest::gtest_main absl::any absl::log absl::base absl::bits)

gtest_discover_tests(
        logging_test capiTests
        DISCOVERY_MODE PRE_TEST
)

if(MSVC)
    add_custom_command(
            TARGET logging_test POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
            $<TARGET_RUNTIME_DLLS:logging_test>
            $<TARGET_FILE_DIR:logging_test>
            COMMAND_EXPAND_LISTS
    )
endif()