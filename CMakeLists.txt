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

add_library(cluster_cxx SHARED src/cluster.cxx include/libcouchbase/cluster.hxx src/bucket.cxx include/libcouchbase/bucket.hxx src/collection.cxx include/libcouchbase/collection.hxx src/result.cxx include/libcouchbase/result.hxx src/mutate_in_spec.cxx include/libcouchbase/mutate_in_spec.hxx src/lookup_in_spec.cxx include/libcouchbase/lookup_in_spec.hxx include/libcouchbase/exceptions.hxx)
add_library(transactions_cxx SHARED src/transactions.cxx include/libcouchbase/transactions.hxx include/libcouchbase/transactions/configuration.hxx include/libcouchbase/transactions/durability_level.hxx include/libcouchbase/transactions/attempt_context.hxx src/attempt_context.cxx src/transaction_document.cxx include/libcouchbase/transactions/transaction_document.hxx src/transaction_context.cxx include/libcouchbase/transactions/transaction_context.hxx src/uid_generator.cxx include/libcouchbase/transactions/uid_generator.hxx src/atr_ids.cxx src/atr_ids.hxx include/libcouchbase/transactions/attempt_state.hxx src/staged_mutation.cxx include/libcouchbase/transactions/staged_mutation.hxx include/libcouchbase/transactions/transaction_fields.hxx src/transaction_links.cxx include/libcouchbase/transactions/transaction_links.hxx include/libcouchbase/transactions/transaction_document_status.hxx src/configuration.cxx)

add_executable(game_server examples/game_server.cxx)
target_link_libraries(game_server transactions_cxx cluster_cxx ${JSON11_LIBRARIES} couchbase)

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
