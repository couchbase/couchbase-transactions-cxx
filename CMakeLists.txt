cmake_minimum_required(VERSION 3.9)

# ============= BEGIN GTEST ============================================

# Download and unpack googletest at configure time
#configure_file(CMakeLists.txt.in ${CMAKE_CURRENT_BINARY_DIR}/googletest-download/CMakeLists.txt)
#execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
#    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/googletest-download"
#)
#execute_process(COMMAND "${CMAKE_COMMAND}" --build .
#    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/googletest-download"
#)
#
#set (CMAKE_CXX_STANDARD 11)
#
# Prevent GoogleTest from overriding our compiler/linker options
# when building with Visual Studio
#set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This adds the following targets:
# gtest, gtest_main, gmock and gmock_main
#add_subdirectory("${CMAKE_CURRENT_BINARY_DIR}/googletest-src"
#                 "${CMAKE_CURRENT_BINARY_DIR}/googletest-build"
#)
# ============= END GTEST ============================================
# ============= BEGIN EXTERNAL DEPS ==================================
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
configure_file(CMakeLists.txt.in ${CMAKE_CURRENT_BINARY_DIR}/spdlog-download/CMakeLists.txt)

execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
                WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/spdlog-download"
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
                WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/spdlog-download"
)

add_subdirectory("${CMAKE_CURRENT_BINARY_DIR}/spdlog-src"
                 "${CMAKE_CURRENT_BINARY_DIR}/spdlog-build"
)
# ============= END EXTERNAL DEPS ====================================
# ============= BEGIN DEPS ===========================================
project(transactions_cxx)
set(CMAKE_CXX_STANDARD 11)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")


option(BUILD_DOC "Build documentation" ON)
option(BUILD_EXAMPLES "Build examples" ON)

set(LCB_NO_TESTS ON)
set(LCB_NO_TOOLS ON)
set(LCB_BUILD_LIBEV OFF)
set(LCB_BUILD_LIBUV OFF)
set(LCB_EMBED_PLUGIN_LIBEVENT ON)
set(LCB_STATIC_LIBEVENT ON)

add_subdirectory(deps/libcouchbase)
add_subdirectory(deps/libcouchbase/contrib/gtest-1.8.1)
add_subdirectory(deps/json)

find_package(Threads)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_LIBS OFF)
include(FindBoost)
find_package(Boost REQUIRED COMPONENTS system thread date_time)
include_directories(BEFORE ${Boost_INCLUDE_DIRS})

message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")
message(STATUS "Boost_LIBRARIES: ${Boost_LIBRARIES}")

#============ END DEPS =======================================================
#============ BEGIN CLIENT ===================================================
include_directories(include ${PROJECT_SOURCE_DIR}/deps/libcouchbase/include
                            ${PROJECT_BINARY_DIR}/deps/libcouchbase/generated
			    ${CMAKE_CURRENT_BINARY_DIR}/spdlog-src/include)

file(GLOB_RECURSE client_include_FILES ${PROJECT_SOURCE_DIR}/include/couchbase/client/*.hxx)
file(GLOB_RECURSE client_src_FILES ${PROJECT_SOURCE_DIR}/src/client/*.cxx)
add_library(client_cxx SHARED ${client_include_FILES} ${client_src_FILES})
target_link_libraries(client_cxx
                      ${CMAKE_THREAD_LIBS_INIT}
                      nlohmann_json::nlohmann_json
                      spdlog
                      ${Boost_LIBRARIES}
                      couchbase)
#============ END CLIENT ====================================================================
# =========== BEGIN TRANSACTIONS ============================================================
file(GLOB_RECURSE transactions_include_FILES ${PROJECT_SOURCE_DIR}/include/couchbase/transactions/*.hxx)
file(GLOB_RECURSE transactions_src_FILES ${PROJECT_SOURCE_DIR}/src/transactions/*.cxx)
add_library(transactions_cxx SHARED ${transactions_include_FILES} ${transactions_src_FILES})
target_link_libraries(transactions_cxx
                      ${CMAKE_THREAD_LIBS_INIT}
                      nlohmann_json::nlohmann_json
                      spdlog
                      ${Boost_LIBRARIES}
                      client_cxx)
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
#========== BEGIN TESTS ==========================================================================

file(GLOB CLIENT_TEST_SOURCES "${PROJECT_SOURCE_DIR}/tests/client/*_t.cpp")
# super fragile - lets try to do better
include_directories(${CURRENT_CMAKE_BINARY_DIR}/deps/libcouchbase/contrib/gtest-1.8.1)
add_executable(client_tests ${CLIENT_TEST_SOURCES})
target_link_libraries(client_tests client_cxx gtest)

