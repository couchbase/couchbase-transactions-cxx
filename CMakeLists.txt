cmake_minimum_required(VERSION 3.14)
project(transactions_cxx)

option(BUILD_DOC "Build documentation" ON)
option(BUILD_EXAMPLES "Build examples" ON)

set(LCB_NO_TESTS ON)
set(LCB_NO_TOOLS ON)
set(LCB_BUILD_LIBEV OFF)
set(LCB_BUILD_LIBUV OFF)
set(LCB_EMBED_PLUGIN_LIBEVENT ON)
set(LCB_STATIC_LIBEVENT ON)
add_subdirectory(deps/libcouchbase)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake/Modules)


set(transactions_BOOST_COMPONENTS system thread date_time log_setup log)
if (BUILD_TESTING)
    list(APPEND transactions_BOOST_COMPONENTS unit_test_framework)
endif ()

find_package(Threads)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_LIBS ON)
include(FindBoost)
find_package(Boost REQUIRED COMPONENTS ${transactions_BOOST_COMPONENTS})
include_directories(BEFORE ${Boost_INCLUDE_DIRS})

message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")
message(STATUS "Boost_LIBRARIES: ${Boost_LIBRARIES}")

find_package(double-conversion REQUIRED)
include_directories(BEFORE ${double-conversion_INCLUDE_DIRS})
message(STATUS "double-conversion_INCLUDE_DIR: ${double-conversion_INCLUDE_DIRS}")
message(STATUS "double-conversion_LIBRARIES: ${double-conversion_LIBRARIES}")

find_package(Folly REQUIRED)
include_directories(BEFORE ${Folly_INCLUDE_DIR})
message(STATUS "Folly_INCLUDE_DIR: ${Folly_INCLUDE_DIR}")
message(STATUS "Folly_LIBRARIES: ${Folly_LIBRARIES}")

include_directories(include deps/libcouchbase/include ${PROJECT_BINARY_DIR}/deps/libcouchbase/generated)

file(GLOB_RECURSE client_include_FILES ${PROJECT_SOURCE_DIR}/include/couchbase/client/*.hxx)
file(GLOB_RECURSE client_src_FILES ${PROJECT_SOURCE_DIR}/src/client/*.cxx)
add_library(client_cxx SHARED ${client_include_FILES} ${client_src_FILES})

file(GLOB_RECURSE transactions_include_FILES ${PROJECT_SOURCE_DIR}/include/couchbase/transactions/*.hxx)
file(GLOB_RECURSE transactions_src_FILES ${PROJECT_SOURCE_DIR}/src/transactions/*.cxx)
add_library(transactions_cxx SHARED ${transactions_include_FILES} ${transactions_src_FILES})

if (BUILD_DOC)
    find_package(Doxygen)
    if (DOXYGEN_FOUND)
        set(DOXYGEN_IN ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in)
        set(DOXYGEN_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
        configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
        add_custom_target(docs ALL
                COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
                WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                COMMENT "Generating API documentation with Doxygen"
                VERBATIM)
    else ()
        message(WARNING "Doxygen need to be installed to generate the doxygen documentation")
    endif ()
endif ()

if (BUILD_EXAMPLES)
    add_subdirectory(examples)
endif ()
