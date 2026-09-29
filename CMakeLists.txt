# Copyright © 2025 CCP ehf.
cmake_minimum_required(VERSION 3.16)

option(BUILD_TESTING "Build and run tests." OFF)
if (BUILD_TESTING)
    list(APPEND VCPKG_MANIFEST_FEATURES "tests")
endif()

project(carbon-grpc VERSION 1.0.0)

include(cmake/CcpBuildConfigurations.cmake)
include(cmake/CcpTargetConfigurations.cmake)

add_subdirectory(src)

# Provide an install target if this is the top level project only
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    # Determine if/when to generate/build documentation. The rules are as follows:
    # - If running locally - Build documentation is OFF by default
    # - If running on a build agent (TeamCity) - Build documentation is ON by default
    # - If -DBUILD_DOCUMENTATION=ON/OFF flag is explicitly set, use it.
    set(BUILD_DOCUMENTATION_DEFAULT_FLAG OFF)
    if(DEFINED ENV{TEAMCITY_VERSION})
        set(BUILD_DOCUMENTATION_DEFAULT_FLAG ON)
    endif()
    message(STATUS "Document generation settings: default=${BUILD_DOCUMENTATION_DEFAULT_FLAG}, option BUILD_DOCUMENTATION=${BUILD_DOCUMENTATION}")
    option(BUILD_DOCUMENTATION "Build Documentation" ${BUILD_DOCUMENTATION_DEFAULT_FLAG})

    if (BUILD_TESTING)
        add_subdirectory(tests/logging)
    endif ()

    if(BUILD_DOCUMENTATION)
        # Set up sphinx build for Python autodoc comments.
        set(SPHINX_SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/doc/source)
        set(SPHINX_BUILD ${CMAKE_CURRENT_BINARY_DIR}/doc/build)

        find_package(Python3 COMPONENTS Interpreter REQUIRED)
        include(cmake/CcpDocsGenerator.cmake)

        create_carbon_docs_sphinx_target(
                PYTHON_EXE ${Python3_EXECUTABLE}
                VENV_NAME docs_generation_venv
                PYTHONPATH_ENV $ENV{PYTHONPATH}
                SPHINX_SOURCE ${SPHINX_SOURCE}
                SPHINX_BUILD ${SPHINX_BUILD}
                DOXYGEN_SRC_FILES ${SRC_FILES}
                SPHINX_TARGET_NAME Sphinx
                DOXYGEN_TARGET_NAME Doxygen
                INSTALL_DESTINATION documentation
        )

        add_dependencies(Sphinx client module)
    endif()

    # Install rule for available public headers
    install(
            TARGETS client module
            EXPORT carbon-grpcTarget
            CONFIGURATIONS ${CMAKE_CONFIGURATION_TYPES}
            ARCHIVE DESTINATION lib
            LIBRARY DESTINATION lib
            RUNTIME DESTINATION bin
    )

    install(EXPORT carbon-grpcTarget
            DESTINATION ${CMAKE_INSTALL_PREFIX}/share/carbon-grpc
            NAMESPACE carbon-grpc::
            FILE carbon-grpc.cmake
    )

    install(FILES carbon-grpcConfig.cmake
            DESTINATION ${CMAKE_INSTALL_PREFIX}/share/carbon-grpc
    )

    install(DIRECTORY include DESTINATION . FILES_MATCHING PATTERN "*.h")
    install(DIRECTORY include DESTINATION . FILES_MATCHING PATTERN "*.hpp")

endif()