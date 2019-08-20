macro(define_example name)
    add_executable(${name} ${name}.cxx)
    target_link_libraries(${name} transactions_cxx client_cxx ${double-conversion_LIBRARIES} ${Folly_LIBRARIES} couchbase)
endmacro()

define_example(game_server)
