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

#pragma once
#include <couchbase/support.hxx>
#include <boost/optional.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/logic/tribool.hpp>

namespace couchbase {

enum class durability_level { none, majority, majority_and_persist_to_active, persist_to_majority };

template <typename T>
class common_options
{
  private:
    boost::optional<std::chrono::milliseconds> timeout_;

  public:
      CB_NODISCARD boost::optional<std::chrono::milliseconds> timeout() { return timeout; }
      T& timeout(std::chrono::milliseconds timeout)
      {
          timeout_ = timeout;
          return *static_cast<T*>(this);
      }
};

template<typename T>
class common_mutate_options: public common_options<T>
{
  private:
    boost::optional<uint64_t> cas_;
    boost::optional<durability_level> durability_;

  public:
    CB_NODISCARD boost::optional<uint64_t> cas() const { return cas_; }
    T& cas(uint64_t cas)
    {
        cas_ = cas;
        return *static_cast<T*>(this);
    }

    CB_NODISCARD boost::optional<durability_level> durability() const { return durability_; }
    T& durability(durability_level level)
    {
        durability_ = level;
        return *static_cast<T*>(this);
    }
};

class get_options : public common_options<get_options>
{
  private:
    // TODO: ponder a clearer way to set expiry.  Perhaps a duration _or_ time point?
    boost::optional<uint32_t> expiry_;
  public:
    CB_NODISCARD boost::optional<uint32_t> expiry() const { return expiry_; }
    get_options& expiry(uint32_t expiry)
    {
        expiry_ = expiry;
        return *this;
    }
};

class upsert_options: public common_mutate_options<upsert_options>
{
};

class replace_options: public common_mutate_options<replace_options>
{
};

class remove_options: public common_mutate_options<remove_options>
{
};

class insert_options: public common_mutate_options<insert_options>
{
};

class lookup_in_options: public common_options<lookup_in_options>
{
  private:
    boost::tribool access_deleted_;

  public:
    CB_NODISCARD boost::tribool access_deleted() const { return access_deleted_; }
    lookup_in_options& access_deleted(boost::tribool access_deleted)
    {
        access_deleted_ = access_deleted;
        return *this;
    }
};

class mutate_in_options : public common_mutate_options<mutate_in_options>
{
  private:
    boost::tribool create_as_deleted_;
    boost::tribool access_deleted_;

  public:
    CB_NODISCARD boost::tribool create_as_deleted() const { return create_as_deleted_; }
    mutate_in_options& create_as_deleted(boost::tribool create_as_deleted)
    {
        create_as_deleted_ = create_as_deleted;
        return *this;
    }
    CB_NODISCARD boost::tribool access_deleted() const { return access_deleted_; }
    mutate_in_options& access_deleted(boost::tribool access_deleted)
    {
        access_deleted_ = access_deleted;
        return *this;
    }
};

}; // namespace couchbase
