#pragma once

#include <string>

#include <nlohmann/json.hpp>

#include <libcouchbase/couchbase.h>

namespace couchbase
{
class collection;

enum class mutate_in_spec_type { MUTATE_IN_UPSERT, MUTATE_IN_INSERT, MUTATE_IN_FULLDOC_INSERT, MUTATE_IN_FULLDOC_UPSERT, REMOVE };

class mutate_in_spec
{
    friend collection;

  public:
    template<typename Content>
    static mutate_in_spec upsert(const std::string& path, const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_UPSERT, path, value);
    }

    template<typename Content>
    static mutate_in_spec insert(const std::string& path, const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_INSERT, path, value);
    }

    template<typename Content>
    static mutate_in_spec fulldoc_insert(const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_FULLDOC_INSERT, value);
    }

    template<typename Content>
    static mutate_in_spec fulldoc_upsert(const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_FULLDOC_UPSERT, value);
    }

    static mutate_in_spec remove(const std::string& path)
    {
        return mutate_in_spec(mutate_in_spec_type::REMOVE, path, "");
    }

    mutate_in_spec& xattr()
    {
        flags_ |= LCB_SUBDOCSPECS_F_XATTRPATH;
        return *this;
    }

    mutate_in_spec& create_path()
    {
        flags_ |= LCB_SUBDOCSPECS_F_MKINTERMEDIATES;
        return *this;
    }

    mutate_in_spec& expand_macro()
    {
        flags_ |= LCB_SUBDOCSPECS_F_XATTR_MACROVALUES;
        return *this;
    }

  private:
    mutate_in_spec_type type_;
    std::string path_;
    std::string value_;
    uint32_t flags_;

    mutate_in_spec(mutate_in_spec_type type, std::string path, const nlohmann::json& value)
      : type_(type)
      , path_(std::move(path))
      , value_(value.dump())
      , flags_(0)
    {
    }
    mutate_in_spec(mutate_in_spec_type type, const nlohmann::json& value)
      : type_(type)
      , path_("")
      , value_(value.dump())
      , flags_(0)
    {
    }
};

} // namespace couchbase
