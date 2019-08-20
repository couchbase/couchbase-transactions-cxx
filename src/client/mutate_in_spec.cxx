#include <couchbase/client/mutate_in_spec.hxx>
#include <utility>
#include <libcouchbase/couchbase.h>
#include <json11.hpp>

namespace cb = couchbase;

cb::mutate_in_spec::mutate_in_spec(mutate_in_spec_type type, std::string path, std::string value)
    : type_(type), path_(std::move(path)), value_(std::move(value)), flags_(0)
{
}

cb::mutate_in_spec::mutate_in_spec(mutate_in_spec_type type, std::string value)
    : type_(type), path_(""), value_(std::move(value)), flags_(0)
{
}

cb::mutate_in_spec cb::mutate_in_spec::upsert(const std::string &path, const json11::Json &value)
{
    return cb::mutate_in_spec(mutate_in_spec_type::MUTATE_IN_UPSERT, path, value.dump());
}

cb::mutate_in_spec cb::mutate_in_spec::insert(const std::string &path, const json11::Json &value)
{
    return cb::mutate_in_spec(mutate_in_spec_type::MUTATE_IN_INSERT, path, value.dump());
}

cb::mutate_in_spec cb::mutate_in_spec::fulldoc_insert(const json11::Json &value)
{
    return cb::mutate_in_spec(mutate_in_spec_type::MUTATE_IN_FULLDOC_INSERT, value.dump());
}

cb::mutate_in_spec cb::mutate_in_spec::fulldoc_upsert(const json11::Json &value)
{
    return cb::mutate_in_spec(mutate_in_spec_type::MUTATE_IN_FULLDOC_UPSERT, value.dump());
}

cb::mutate_in_spec cb::mutate_in_spec::remove(const std::string &path)
{
    return cb::mutate_in_spec(mutate_in_spec_type::REMOVE, path, "");
}

cb::mutate_in_spec &cb::mutate_in_spec::xattr()
{
    flags_ |= LCB_SUBDOCOPS_F_XATTRPATH;
    return *this;
}

cb::mutate_in_spec &cb::mutate_in_spec::create_path()
{
    flags_ |= LCB_SUBDOCOPS_F_MKINTERMEDIATES;
    return *this;
}

cb::mutate_in_spec &cb::mutate_in_spec::expand_macro()
{
    flags_ |= LCB_SUBDOCOPS_F_XATTR_MACROVALUES;
    return *this;
}
