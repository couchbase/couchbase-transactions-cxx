#pragma once

#include <string>

namespace couchbase
{
class Collection;

enum MutateInSpecType { MUTATE_IN_UPSERT, MUTATE_IN_INSERT, MUTATE_IN_FULLDOC_INSERT, MUTATE_IN_FULLDOC_UPSERT };

class MutateInSpec
{
    friend Collection;

  public:
    static MutateInSpec upsert(const std::string &path, const std::string &value);
    static MutateInSpec insert(const std::string &path, const std::string &value);
    static MutateInSpec fulldoc_insert(const std::string &value);
    static MutateInSpec fulldoc_upsert(const std::string &value);

    MutateInSpec &xattr();
    MutateInSpec &create_path();
    MutateInSpec &expand_macro();

  private:
    MutateInSpecType type_;
    std::string path_;
    std::string value_;
    uint32_t flags_;

    MutateInSpec(MutateInSpecType type, const std::string &path, const std::string &value);
    MutateInSpec(MutateInSpecType type, const std::string &value);
};

} // namespace couchbase
