#pragma once

#include <string>

namespace couchbase
{
class collection;

enum mutate_in_spec_type { MUTATE_IN_UPSERT, MUTATE_IN_INSERT, MUTATE_IN_FULLDOC_INSERT, MUTATE_IN_FULLDOC_UPSERT };

class mutate_in_spec
{
    friend collection;

  public:
    static mutate_in_spec upsert(const std::string &path, const std::string &value);
    static mutate_in_spec insert(const std::string &path, const std::string &value);
    static mutate_in_spec fulldoc_insert(const std::string &value);
    static mutate_in_spec fulldoc_upsert(const std::string &value);

    mutate_in_spec &xattr();
    mutate_in_spec &create_path();
    mutate_in_spec &expand_macro();

  private:
    mutate_in_spec_type type_;
    std::string path_;
    std::string value_;
    uint32_t flags_;

    mutate_in_spec(mutate_in_spec_type type, const std::string &path, const std::string &value);
    mutate_in_spec(mutate_in_spec_type type, const std::string &value);
};

} // namespace couchbase
