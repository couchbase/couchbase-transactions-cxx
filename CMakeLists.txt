#
#     Copyright 2021 Couchbase, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

cmake_minimum_required(VERSION 3.17)
if (POLICY CMP0063) # Visibility
    cmake_policy(SET CMP0063 NEW)
endif (POLICY CMP0063)

# Use bigobj on windows to enable builds to succeed.
if(WIN32)
  add_definitions(/bigobj)
endif()

# ============= BEGIN EXTERNAL DEPS ==================================
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(ALLOW_DUPLICATE_CUSTOM_TARGETS TRUE)

# ============= END EXTERNAL DEPS ====================================
# ============= BEGIN DEPS ===========================================
project(transactions_cxx)
set(CB_VERSION_MAJOR 2)
set(CB_VERSION_MINOR 0)
set(CB_VERSION_PATCH 0)
set(CB_VERSION_TWEAK "beta.4")

find_program(GIT_EXECUTABLE NAMES git git.exe)
macro(RUNGIT outvar)
    message(STATUS "Getting SHA...")
    execute_process(COMMAND git ${ARGN}
        OUTPUT_VARIABLE ${outvar}
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        OUTPUT_STRIP_TRAILING_WHITESPACE)
endmacro()
if(GIT_EXECUTABLE)
    RUNGIT(CB_VERSION_SHA rev-parse --short HEAD)
endif()

if (CB_VERSION_TWEAK STREQUAL "")
    set(CB_VERSION_STRING ${CB_VERSION_MAJOR}.${CB_VERSION_MINOR}.${CB_VERSION_PATCH})
else()
    set(CB_VERSION_STRING ${CB_VERSION_MAJOR}.${CB_VERSION_MINOR}.${CB_VERSION_PATCH}.${CB_VERSION_TWEAK})
endif()
configure_file(${PROJECT_SOURCE_DIR}/include/couchbase/support.hxx.in ${PROJECT_SOURCE_DIR}/include/couchbase/support.hxx)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")


option(COUCHBASE_TXNS_CXX_BUILD_DOC "Build documentation" ON)
option(COUCHBASE_TXNS_CXX_BUILD_EXAMPLES "Build examples" ON)
option(COUCHBASE_TXNS_CXX_BUILD_TESTS "Build tests" ON)
option(COUCHBASE_TXNS_CXX_CLIENT_EXTERNAL "Use external couchbase-cxx-client library instead of bundled" OFF)

set(JSON_BuildTests OFF CACHE INTERNAL "")

if(COUCHBASE_TXNS_CXX_BUILD_TESTS)
    add_subdirectory(deps/gtest/)
endif()

configure_file(
    ${PROJECT_SOURCE_DIR}/deps/json/single_include/nlohmann/json.hpp
    ${PROJECT_SOURCE_DIR}/include/couchbase/internal/nlohmann/json.hpp
    COPYONLY)

if(NOT COUCHBASE_TXNS_CXX_CLIENT_EXTERNAL)
    if (APPLE AND NOT DEFINED OPENSSL_ROOT_DIR)
        # look in typical brew location - missed by the openssl finder
        message(STATUS "no OPENSSL_ROOT_DIR specified, using MacOS, so defaulting to homebrew location")
        set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl@1.1", CACHE INTERNAL "")
    endif()
    message(STATUS "APPLE: '${APPLE}', SSL: '${OPENSSL_ROOT_DIR}'")
    find_package(OpenSSL REQUIRED)
    set(COUCHBASE_CXX_CLIENT_BUILD_TESTS OFF CACHE INTERNAL "")
    add_subdirectory(deps/couchbase-cxx-client)
endif()

find_package(Threads)

#============ END DEPS =======================================================

# =========== BEGIN TRANSACTIONS ===========================================================
include_directories(include)
file(GLOB_RECURSE transactions_include_FILES ${PROJECT_SOURCE_DIR}/include/couchbase/transactions/*.hxx)
file(GLOB_RECURSE transactions_src_FILES ${PROJECT_SOURCE_DIR}/src/transactions/*.cxx)
if(COUCHBASE_TXNS_CXX_BUILD_SHARED)
  add_library(transactions_cxx SHARED ${transactions_include_FILES} ${transactions_src_FILES})
else()
  add_library(transactions_cxx STATIC ${transactions_include_FILES} ${transactions_src_FILES})
endif()
target_link_libraries(transactions_cxx
                      ${CMAKE_THREAD_LIBS_INIT}
                      couchbase_cxx_client)

set_target_properties(transactions_cxx PROPERTIES VERSION ${CB_VERSION_STRING} SOVERSION ${CB_VERSION_MAJOR})
# =========== END TRANSACTIONS ================================================================

# =========== BEGIN DOCS ======================================================================
if(COUCHBASE_TXNS_CXX_BUILD_DOC)
    find_package(Doxygen)
    if(DOXYGEN_FOUND)
        set(DOXYGEN_IN ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in)
        set(DOXYGEN_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
        configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
        add_custom_target(docs ALL
                          COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
                          WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                          COMMENT "Generating API documentation with Doxygen"
                          VERBATIM)
    else()
        message(WARNING "Doxygen need to be installed to generate the doxygen documentation")
    endif()
endif()
#=========== END DOCS ============================================================================
#=========== BEGIN EXAMPLES ======================================================================
if(COUCHBASE_TXNS_CXX_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
#========== END EXAMPLES =========================================================================
#=========== BEGIN TARBALL =======================================================================
set(tarball_name "couchbase-transactions-${CB_VERSION_STRING}")
set(tarball_manifest_path "${CMAKE_CURRENT_BINARY_DIR}/tarball-manifest.txt")
set(tarball_exclude_path "${PROJECT_SOURCE_DIR}/scripts/jenkins/pkg/exclude.txt")

add_custom_target(tarball_manifest
    COMMAND sh -c 'test -e ${tarball_manifest_path} || git ls-files --recurse-submodules | grep --invert-match --file=${tarball_exclude_path} > ${tarball_manifest_path}'
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

add_custom_target(tarball
    COMMAND rm -rf "${tarball_name}"
    COMMAND mkdir "${tarball_name}"
    COMMAND tar -c -f - --directory "${PROJECT_SOURCE_DIR}" --files-from "${tarball_manifest_path}" | tar -x -f - --directory "${tarball_name}"
    COMMAND cp "${PROJECT_SOURCE_DIR}/deps/libcouchbase/packaging/distinfo/distinfo.cmake" "${tarball_name}/deps/libcouchbase/packaging/distinfo/"
    COMMAND tar -cz -f "${tarball_name}.tar.gz" "${tarball_name}"
    COMMAND rm -rf "${tarball_name}"
    DEPENDS tarball_manifest)

include(GNUInstallDirs)
install(TARGETS transactions_cxx DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES ${CMAKE_SOURCE_DIR}/deps/json/single_include/nlohmann/json.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/couchbase/internal/nlohmann)
install(DIRECTORY include/couchbase
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING PATTERN *.hxx)
#=========== END TARBALL =========================================================================

#========== BEGIN TESTS ==========================================================================

if(COUCHBASE_TXNS_CXX_BUILD_TESTS)
    file(GLOB_RECURSE CLIENT_TEST_SOURCES "${PROJECT_SOURCE_DIR}/tests/transactions/*.cpp")
    include_directories(${CURRENT_CMAKE_BINARY_DIR}/deps/gtest)
    add_executable(client_tests ${CLIENT_TEST_SOURCES})
    target_link_libraries(client_tests transactions_cxx gtest)
endif()
