# Usage of this module as follows:
#
#     find_package(Folly)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  FOLLY_ROOT_DIR Set this variable to the root installation of
#                    folly if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  Folly_FOUND             System has folly libs/headers
#  Folly_LIBRARIES         The folly library/libraries
#  Folly_INCLUDE_DIR       The location of folly headers

find_path(Folly_ROOT_DIR
        NAMES include/folly/folly-config.h
        )

find_library(Folly_LIBRARIES
        NAMES folly
        HINTS ${Folly_ROOT_DIR}/lib
        )

find_library(Folly_BENCHMARK_LIBRARIES
        NAMES follybenchmark
        HINTS ${Folly_ROOT_DIR}/lib
        )

find_path(Folly_INCLUDE_DIRS
        NAMES folly/folly-config.h
        HINTS ${Folly_ROOT_DIR}/include
        )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Folly DEFAULT_MSG
        Folly_LIBRARIES
        Folly_INCLUDE_DIRS
        )

mark_as_advanced(
        Folly_ROOT_DIR
        Folly_LIBRARIES
        Folly_BENCHMARK_LIBRARIES
        Folly_INCLUDE_DIRS
)
