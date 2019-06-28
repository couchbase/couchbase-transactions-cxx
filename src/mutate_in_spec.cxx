#include <libcouchbase/mutate_in_spec.hxx>
#include <libcouchbase/couchbase.h>

couchbase::MutateInSpec::MutateInSpec(MutateInSpecType type, const std::string &path, const std::string &value)
    : type_(type), path_(path), value_(value), flags_(0)
{
}

couchbase::MutateInSpec::MutateInSpec(MutateInSpecType type, const std::string &value) : type_(type), path_(""), value_(value), flags_(0)
{
}

couchbase::MutateInSpec couchbase::MutateInSpec::upsert(const std::string &path, const std::string &value)
{
    return couchbase::MutateInSpec(MutateInSpecType::MUTATE_IN_UPSERT, path, value);
}

couchbase::MutateInSpec couchbase::MutateInSpec::insert(const std::string &path, const std::string &value)
{
    return couchbase::MutateInSpec(MutateInSpecType::MUTATE_IN_INSERT, path, value);
}

couchbase::MutateInSpec couchbase::MutateInSpec::fulldoc_insert(const std::string &value)
{
    return couchbase::MutateInSpec(MutateInSpecType::MUTATE_IN_FULLDOC_INSERT, value);
}

couchbase::MutateInSpec couchbase::MutateInSpec::fulldoc_upsert(const std::string &value)
{
    return couchbase::MutateInSpec(MutateInSpecType::MUTATE_IN_FULLDOC_UPSERT, value);
}

couchbase::MutateInSpec &couchbase::MutateInSpec::xattr()
{
    flags_ |= LCB_SUBDOCOPS_F_XATTRPATH;
    return *this;
}

couchbase::MutateInSpec &couchbase::MutateInSpec::create_path()
{
    flags_ |= LCB_SUBDOCOPS_F_MKINTERMEDIATES;
    return *this;
}

couchbase::MutateInSpec &couchbase::MutateInSpec::expand_macro()
{
    flags_ |= LCB_SUBDOCOPS_F_XATTR_MACROVALUES;
    return *this;
}
