#include <libcouchbase/mutate_in_spec.hxx>
#include <utility>
#include <libcouchbase/couchbase.h>

couchbase::mutate_in_spec::mutate_in_spec(mutate_in_spec_type type, std::string path, std::string value)
    : type_(type), path_(std::move(path)), value_(std::move(value)), flags_(0)
{
}

couchbase::mutate_in_spec::mutate_in_spec(mutate_in_spec_type type, std::string value)
    : type_(type), path_(""), value_(std::move(value)), flags_(0)
{
}

couchbase::mutate_in_spec couchbase::mutate_in_spec::upsert(const std::string &path, const std::string &value)
{
    return couchbase::mutate_in_spec(mutate_in_spec_type::MUTATE_IN_UPSERT, path, value);
}

couchbase::mutate_in_spec couchbase::mutate_in_spec::insert(const std::string &path, const std::string &value)
{
    return couchbase::mutate_in_spec(mutate_in_spec_type::MUTATE_IN_INSERT, path, value);
}

couchbase::mutate_in_spec couchbase::mutate_in_spec::fulldoc_insert(const std::string &value)
{
    return couchbase::mutate_in_spec(mutate_in_spec_type::MUTATE_IN_FULLDOC_INSERT, value);
}

couchbase::mutate_in_spec couchbase::mutate_in_spec::fulldoc_upsert(const std::string &value)
{
    return couchbase::mutate_in_spec(mutate_in_spec_type::MUTATE_IN_FULLDOC_UPSERT, value);
}

couchbase::mutate_in_spec &couchbase::mutate_in_spec::xattr()
{
    flags_ |= LCB_SUBDOCOPS_F_XATTRPATH;
    return *this;
}

couchbase::mutate_in_spec &couchbase::mutate_in_spec::create_path()
{
    flags_ |= LCB_SUBDOCOPS_F_MKINTERMEDIATES;
    return *this;
}

couchbase::mutate_in_spec &couchbase::mutate_in_spec::expand_macro()
{
    flags_ |= LCB_SUBDOCOPS_F_XATTR_MACROVALUES;
    return *this;
}
