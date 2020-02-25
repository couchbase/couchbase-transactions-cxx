macro(define_example name)
    add_executable(${name} ${name}.cxx)
    target_link_libraries(${name} ${CMAKE_THREAD_LIBS_INIT} transactions_cxx client_cxx ${Boost_LIBRARIES} couchbase)
endmacro()

define_example(game_server)
