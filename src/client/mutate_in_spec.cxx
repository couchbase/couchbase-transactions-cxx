/*
 *     Copyright 2021 Couchbase, Inc.
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

#include <libcouchbase/couchbase.h>

#include <couchbase/client/mutate_in_spec.hxx>

couchbase::mutate_in_spec&
couchbase::mutate_in_spec::xattr()
{
    flags_ |= LCB_SUBDOCSPECS_F_XATTRPATH;
    return *this;
}

couchbase::mutate_in_spec&
couchbase::mutate_in_spec::create_path()
{
    flags_ |= LCB_SUBDOCSPECS_F_MKINTERMEDIATES;
    return *this;
}

couchbase::mutate_in_spec&
couchbase::mutate_in_spec::expand_macro()
{
    flags_ |= LCB_SUBDOCSPECS_F_XATTR_MACROVALUES;
    return *this;
}
