/*
 *     Copyright 2020 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/** Helpers to avoid needless warnings */
#pragma once
/* C++ language standard detection - helpful later on... */
#if (defined(__cplusplus) && __cplusplus >= 201703L) || (defined(_HAS_CXX17) && _HAS_CXX17 == 1)
#define CB_HAS_CPP_17
#define CB_HAS_CPP_14
#elif (defined(__cplusplus) && __cplusplus >= 201402L) || (defined(_HAS_CXX14) && _HAS_CXX14 == 1)
#define CB_HAS_CPP_14
#endif

/* define a portable, warning-free nodiscard - CB_NODISCARD */
#if defined(__has_cpp_attribute)
#if __has_cpp_attribute(nodiscard)
#if defined(__clang__) && !defined(CB_HAS_CPP_17)
#define CB_NODISCARD
#else
#define CB_NODISCARD CB_NODISCARD
#endif
#elif __has_cpp_attribute(gnu::warn_unused_result)
#define CB_NODISCARD [[gnu::warn_unused_result]]
#else
#define CB_NODISCARD
#endif
#else
#define CB_NODISCARD
#endif
