cmake_minimum_required(VERSION 3.9)
if (POLICY CMP0063) # Visibility
    cmake_policy(SET CMP0063 NEW)
endif (POLICY CMP0063)

# ============= BEGIN EXTERNAL DEPS ==================================
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(ALLOW_DUPLICATE_CUSTOM_TARGETS TRUE)

# ============= END EXTERNAL DEPS ====================================
# ============= BEGIN DEPS ===========================================
project(transactions_cxx)
set(CB_VERSION_MAJOR 1)
set(CB_VERSION_MINOR 0)
set(CB_VERSION_PATCH "0.beta")
set(CB_VERSION_TWEAK 2)
set(CB_VERSION_STRING ${CB_VERSION_MAJOR}.${CB_VERSION_MINOR}.${CB_VERSION_PATCH}.${CB_VERSION_TWEAK})

find_program(GIT_EXECUTABLE NAMES git git.exe)
macro(RUNGIT outvar)
    message(STATUS "Getting SHA...")
    execute_process(COMMAND git ${ARGN}
        OUTPUT_VARIABLE ${outvar}
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        OUTPUT_STRIP_TRAILING_WHITESPACE)
endmacro()
if(GIT_EXECUTABLE)
    RUNGIT(CB_VERSION_SHA rev-parse HEAD)
endif()
configure_file(${PROJECT_SOURCE_DIR}/include/couchbase/support.hxx.in ${PROJECT_SOURCE_DIR}/include/couchbase/support.hxx)

set(CMAKE_CXX_STANDARD 11)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")


option(BUILD_DOC "Build documentation" ON)
option(BUILD_EXAMPLES "Build examples" ON)
option(BUILD_TESTS "Build tests" ON)
option(STATIC_BOOST "Link Boost libraries statically" OFF)

set(LCB_NO_TESTS ON CACHE INTERNAL "")
set(LCB_NO_TOOLS ON CACHE INTERNAL "")
set(LCB_BUILD_LIBEV OFF CACHE INTERNAL "")
set(LCB_BUILD_LIBUV OFF CACHE INTERNAL "")
set(LCB_EMBED_PLUGIN_LIBEVENT ON CACHE INTERNAL "")
set(LCB_STATIC_LIBEVENT ON CACHE INTERNAL "")
set(LCB_BUILD_DTRACE OFF CACHE INTERNAL "")
set(LCB_INSTALL_HEADERS OFF CACHE INTERNAL "")
set(LCB_INSTALL_PKGCONFIG OFF CACHE INTERNAL "")
set(LCB_INSTALL_LIBRARY OFF CACHE INTERNAL "")

add_definitions(-DLIBCOUCHBASE_STATIC=1)
add_subdirectory(deps/libcouchbase)
if(BUILD_TESTS)
    add_subdirectory(deps/libcouchbase/contrib/gtest-1.8.1)
endif()

configure_file(
    ${PROJECT_SOURCE_DIR}/deps/json/single_include/nlohmann/json.hpp
    ${PROJECT_SOURCE_DIR}/include/couchbase/internal/nlohmann/json.hpp
    COPYONLY)

add_subdirectory(deps/spdlog)

find_package(Threads)
# Just in case of debugging CMake recipes for Boost
# set(Boost_DEBUG ON)
# set(Boost_VERBOSE ON)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_LIBS ${STATIC_BOOST})
find_package(Boost REQUIRED COMPONENTS system thread date_time)
include_directories(BEFORE ${Boost_INCLUDE_DIRS})

message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")
message(STATUS "Boost_LIBRARIES: ${Boost_LIBRARIES}")

#============ END DEPS =======================================================
#============ BEGIN CLIENT ===================================================
include_directories(include)
include_directories(BEFORE
        ${PROJECT_SOURCE_DIR}/deps/spdlog/include
        ${PROJECT_BINARY_DIR}/generated/json/include
        ${PROJECT_SOURCE_DIR}/deps/libcouchbase/include
        ${PROJECT_BINARY_DIR}/deps/libcouchbase/generated)

file(GLOB_RECURSE client_include_FILES ${PROJECT_SOURCE_DIR}/include/couchbase/client/*.hxx)
file(GLOB_RECURSE client_src_FILES ${PROJECT_SOURCE_DIR}/src/client/*.cxx)
add_definitions(-DBOOST_UUID_RANDOM_GENERATOR_COMPAT)
add_library(client_cxx STATIC ${client_include_FILES} ${client_src_FILES})
set_property(TARGET client_cxx PROPERTY C_VISIBILITY_PRESET hidden)
# Do not hide C++ API of client_cxx, as user might want to use it
# set_property(TARGET client_cxx PROPERTY CXX_VISIBILITY_PRESET hidden)
set_property(TARGET client_cxx PROPERTY VISIBILITY_INLINES_HIDDEN ON)
target_link_libraries(client_cxx
                      PRIVATE
                      ${CMAKE_THREAD_LIBS_INIT}
                      spdlog::spdlog
                      ${Boost_LIBRARIES}
                      couchbaseS)
set_target_properties(client_cxx PROPERTIES VERSION ${CB_VERSION_STRING} SOVERSION ${CB_VERSION_MAJOR})
#============ END CLIENT ====================================================================
# =========== BEGIN TRANSACTIONS ============================================================
file(GLOB_RECURSE transactions_include_FILES ${PROJECT_SOURCE_DIR}/include/couchbase/transactions/*.hxx)
file(GLOB_RECURSE transactions_src_FILES ${PROJECT_SOURCE_DIR}/src/transactions/*.cxx)
add_library(transactions_cxx SHARED ${transactions_include_FILES} ${transactions_src_FILES})
target_link_libraries(transactions_cxx
                      ${CMAKE_THREAD_LIBS_INIT}
                      spdlog::spdlog
                      ${Boost_LIBRARIES}
                      client_cxx)
set_target_properties(transactions_cxx PROPERTIES VERSION ${CB_VERSION_STRING} SOVERSION ${CB_VERSION_MAJOR})
# =========== END TRANSACTIONS ================================================================
# =========== BEGIN DOCS ======================================================================
if(BUILD_DOC)
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
if(BUILD_EXAMPLES)
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

if(BUILD_TESTS)
    file(GLOB_RECURSE CLIENT_TEST_SOURCES "${PROJECT_SOURCE_DIR}/tests/*_t.cpp")
    # super fragile - lets try to do better
    include_directories(${CURRENT_CMAKE_BINARY_DIR}/deps/libcouchbase/contrib/gtest-1.8.1)
    add_executable(client_tests ${CLIENT_TEST_SOURCES})
    target_link_libraries(client_tests transactions_cxx gtest)
endif()
