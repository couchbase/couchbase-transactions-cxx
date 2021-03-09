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
#include <boost/logic/tribool.hpp>
#include <boost/optional.hpp>
#include <boost/optional/optional_io.hpp>
#include <chrono>
#include <couchbase/support.hxx>

/**
 * @file
 * Provides options objects for all the kv operations in Couchbase Transactions Client.
 */
namespace couchbase
{

/**
 * @brief KV Write Durability
 *
 * When writing a document (insert, upsert, replace, mutate_in), we may
 * want the opreation to not return until the document is in memory, or
 * on disk, within the cluster.
 */
enum class durability_level {
    none,                           /**< Primary node has the mutation in-memory (this is the fastest, and the default). */
    majority,                       /**< a majority of nodes have the document in-memory. */
    majority_and_persist_to_active, /**< a majority of nodes have the document in-memory, and the primary has persisted it to disk */
    persist_to_majority             /**< a mojority of nodes have persisted the document to disk.*/
};

/**
 * @brief store semantics for subdoc mutations
 *
 * When not specified, the semantics are inferred from the @ref mutate_in_spec in the mutate_in operation.  However,
 * if needed, it can be specified.
 */
enum class subdoc_store_semantics {
    upsert, /**< If document doesn't exist, insert it.  Otherwise, update */
    insert, /**< If document exists, return a LCB_ERR_DOCUMENT_EXISTS */
    replace /**< If document doesn't exist, return LCB_ERR_DOCUMENT_NOT_FOUND */
};

/**
 *  @brief base class for all options
 *
 *  Contains options common to all operations.
 */
template<typename T>
class common_options
{
  private:
    boost::optional<std::chrono::microseconds> timeout_;

  public:
    /**
     *  @brief get timeout
     *
     * Get the timeout set in this object.
     *
     * @return The timeout value.
     */
    CB_NODISCARD boost::optional<std::chrono::microseconds> timeout() const
    {
        return timeout_;
    }
    /**
     *  @brief Set timeout
     *
     * @param timeout Set the timeout for this option object.
     * @return reference to this options class.  Makes these calls chainable.
     */
    template<typename R>
    T& timeout(R timeout)
    {
        timeout_ = std::chrono::duration_cast<std::chrono::microseconds>(timeout);
        return *static_cast<T*>(this);
    }
};

/**
 *  @brief Options common to mutation operations
 */
template<typename T>
class common_mutate_options : public common_options<T>
{
  private:
    boost::optional<uint64_t> cas_;
    boost::optional<durability_level> durability_;

  public:
    /**
     *  @brief Get cas
     *
     * @returns The cas, if set.
     */
    CB_NODISCARD boost::optional<uint64_t> cas() const
    {
        return cas_;
    }
    /**
     * @brief Set current CAS
     *
     * @param cas the current CAS of the document.  Mutation will fail with a
     *            result code of LCB_ERR_CAS_MISMATCH when this doesn't match the
     *            current CAS of the document.  Ignored if 0.
     * @return A reference to this object, so the calls can be chained.
     */
    T& cas(uint64_t cas)
    {
        cas_ = cas;
        return *static_cast<T*>(this);
    }

    /**
     *  @brief Get Durability
     *
     * @return the @ref durability_level set in this object.
     */
    CB_NODISCARD boost::optional<durability_level> durability() const
    {
        return durability_;
    }
    /**
     * @brief Set durability
     *
     * @param level Desired @ref durability_level for the mutation operation.
     * @return Reference to this object, so calls can be chained.
     */
    T& durability(durability_level level)
    {
        durability_ = level;
        return *static_cast<T*>(this);
    }
};

/**
 * @brief Options for @ref collection.get()
 */
class get_options : public common_options<get_options>
{
  private:
    // TODO: ponder a clearer way to set expiry.  Perhaps a duration _or_ time point?
    boost::optional<uint32_t> expiry_;

  public:
    /**
     * @brief Get expiry
     * @volatile
     * @return expiry set in this object, if any.
     */
    CB_NODISCARD boost::optional<uint32_t> expiry() const
    {
        return expiry_;
    }
    /**
     * @brief Set expiry
     * @volatile
     *
     * This will perform a 'get and touch', updating the exipry and returning the document,
     * when set.  The expiry is a uint32_t representing either the number of seconds from
     * now that the document will expire, or the epoch seconds that the expiry should occur.
     * If the value is less than 30 days (30*24*60*60), it is assumed to be the former, otherwisw
     * the latter.  Note that from 30 days to today, the docuent would immediately expire.
     *
     * @param expiry Set the desired expiry for this option.
     * @return Reference to this object, so calls can be chained.
     */
    get_options& expiry(uint32_t expiry)
    {
        expiry_ = expiry;
        return *this;
    }
};

/**
 * @brief Options for @ref collection.exists()
 */
class exists_options : public common_options<exists_options>
{
};

/**
 * @brief Options for @ref collection.upsert()
 */
class upsert_options : public common_mutate_options<upsert_options>
{
};

/**
 * @brief Options for @ref collection.replace()
 */
class replace_options : public common_mutate_options<replace_options>
{
};

/**`
 * @brief Options for @ref collection.remove()
 */
class remove_options : public common_mutate_options<remove_options>
{
};

/**`
 * @brief Options for @ref collection.insert()
 */
class insert_options : public common_mutate_options<insert_options>
{
};

/**`
 * @brief Options for @ref collection.lookup_in()
 */
class lookup_in_options : public common_options<lookup_in_options>
{
  private:
    boost::tribool access_deleted_;

  public:
    /**
     * @brief Get access deleted flag
     *
     * When true, recently deleted documents can be read.  Used internally in conjunction
     * with @ref create_deleted.
     *
     *  @return The access_deleted flag.
     */
    CB_NODISCARD boost::tribool access_deleted() const
    {
        return access_deleted_;
    }
    /**
     * @brief Set access_deleted flag
     *
     * All operations ignore deleted documents, unless this is set to true.
     *
     * @param access_deleted Desired state for @ref access_deleted.
     * @return Reference to this object, so calls can be chained.
     */
    lookup_in_options& access_deleted(boost::tribool access_deleted)
    {
        access_deleted_ = access_deleted;
        return *this;
    }
};

/**`
 * @brief Options for @ref collection.mutate_in()
 */
class mutate_in_options : public common_mutate_options<mutate_in_options>
{
  private:
    boost::tribool create_as_deleted_;
    boost::tribool access_deleted_;
    boost::optional<subdoc_store_semantics> store_semantics_;

  public:
    /**
     * @brief Get create_as_deleted flag
     *
     * @return The state of the create_as_deleted flag.
     */
    CB_NODISCARD boost::tribool create_as_deleted() const
    {
        return create_as_deleted_;
    }
    /**
     * @brief Set create_as_deleted
     *
     * When you specify create_as_deleted, the document will be created as if it was already
     * deleted.  Used in conjunction with the access_deleted flag internally.
     *
     * @param create_as_deleted Desired state of create_as_deleted flag.
     * @return Reference to this object, so calls can be chained.
     */
    mutate_in_options& create_as_deleted(boost::tribool create_as_deleted)
    {
        create_as_deleted_ = create_as_deleted;
        return *this;
    }
    /**
     * @brief Get access deleted flag
     *
     * When true, deleted documents can be read.  Used internally in conjunction
     * with @ref lookup_in_options.create_deleted().
     *
     *  @return The access_deleted flag.
     */
    CB_NODISCARD boost::tribool access_deleted() const
    {
        return access_deleted_;
    }
    /**
     * @brief Set access_deleted flag.
     *
     * All operations ignore deleted documents, unless this is set to true.
     *
     * @param access_deleted Desired state for access_deleted.     * @return Reference to this object, so calls can be chained.
     */
    mutate_in_options& access_deleted(boost::tribool access_deleted)
    {
        access_deleted_ = access_deleted;
        return *this;
    }
    /**
     * @brief Get store semantics
     *
     * When upsert, the mutation should create a new doc if one doesn't exist, otherwise just mutates existing doc.
     * When insert, the mutation should create a new doc only if it doesn't exist, otherwise returns LCB_ERR_DOCUMENT_EXISTS.
     * When replace, the mutation should mutate an existing doc, if the document doesn't exist, the operation will return LCB_ERR_DOCUMENT_NOT_FOUND.
     * @return The store semantics, if set.
     */
    CB_NODISCARD boost::optional<subdoc_store_semantics> store_semantics() const
    {
        return store_semantics_;
    }
    /**
     * @brief Set the store semantics
     *
     * When upsert, the mutation should create a new doc if one doesn't exist, otherwise just mutates existing doc.
     * When insert, the mutation should create a new doc only if it doesn't exist, otherwise returns LCB_ERR_DOCUMENT_EXISTS.
     * When replace, the mutation should mutate an existing doc, if the document doesn't exist, the operation will return LCB_ERR_DOCUMENT_NOT_FOUND.
     *
     * @param _semantics Desired state for the semantics.  Note this overrides any implied semantics that may
     *                        be inferred by the specs (@ref mutate_in_spec::fulldoc_insert, @ref mutate_in_spec::fulldoc_upsert
     *                        for instance).
     * @return Reference to this object, so calls can be chained.
     */
    mutate_in_options& store_semantics(subdoc_store_semantics semantics)
    {
        store_semantics_ = semantics;
        return *this;
    }
};

}; // namespace couchbase
