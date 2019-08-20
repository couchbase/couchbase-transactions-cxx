cmake_minimum_required(VERSION 3.14)
project(transactions_cxx)

option(BUILD_DOC "Build documentation" ON)

set(LCB_NO_TESTS ON)
set(LCB_NO_TOOLS ON)
set(LCB_BUILD_LIBEV OFF)
set(LCB_BUILD_LIBUV OFF)
set(LCB_EMBED_PLUGIN_LIBEVENT ON)
set(LCB_STATIC_LIBEVENT ON)
add_subdirectory(deps/libcouchbase)

set(CMAKE_CXX_STANDARD 17)

include(FindPkgConfig)
pkg_check_modules(JSON11 REQUIRED json11>=1.0)
message(STATUS "found json11: ${JSON11_LDFLAGS};${JSON11_CFLAGS}")

include_directories(include deps/libcouchbase/include ${PROJECT_BINARY_DIR}/deps/libcouchbase/generated)

file(GLOB_RECURSE client_include_FILES ${PROJECT_SOURCE_DIR}/include/couchbase/client/*.hxx)
file(GLOB_RECURSE client_src_FILES ${PROJECT_SOURCE_DIR}/src/client/*.cxx)
add_library(client_cxx SHARED ${client_include_FILES} ${client_src_FILES})

file(GLOB_RECURSE transactions_include_FILES ${PROJECT_SOURCE_DIR}/include/couchbase/transactions/*.hxx)
file(GLOB_RECURSE transactions_src_FILES ${PROJECT_SOURCE_DIR}/src/transactions/*.cxx)
add_library(transactions_cxx SHARED ${transactions_include_FILES} ${transactions_src_FILES})

add_executable(game_server examples/game_server.cxx)
target_link_libraries(game_server transactions_cxx client_cxx ${JSON11_LIBRARIES} couchbase)

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
    else()
        message(WARNING "Doxygen need to be installed to generate the doxygen documentation")
    endif()
endif()
