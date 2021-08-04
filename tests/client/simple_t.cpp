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

#include <chrono>
#include <memory>

#include "../../src/client/cluster.cxx" // to get the private instance_pool_event_counter
#include "client_env.h"
#include "helpers.hxx"
#include <couchbase/client/cluster.hxx>
#include <couchbase/client/collection.hxx>
#include <gtest/gtest.h>
#include <libcouchbase/couchbase.h>
#include <spdlog/spdlog.h>

using namespace couchbase;

static std::string bucket_name = std::string("default");
static nlohmann::json content = nlohmann::json::parse("{\"some\":\"thing\"}");

static void
upsert_random_doc(std::shared_ptr<couchbase::collection>& coll, std::string& id)
{
    id = ClientTestEnvironment::get_uuid();
    if (!coll) {
        auto c = ClientTestEnvironment::get_cluster();
        auto b = c->bucket(bucket_name);
        coll = b->default_collection();
    }
    auto result = coll->upsert(id, content);
    ASSERT_TRUE(result.is_success());
    ASSERT_EQ(result.rc, 0);
    ASSERT_FALSE(result.is_not_found());
    ASSERT_FALSE(result.is_value_too_large());
    ASSERT_TRUE(result.strerror().find("LCB_SUCCESS") != std::string::npos);
    ASSERT_EQ(result.key, id);
    ASSERT_FALSE(result.has_value());
}

TEST(SimpleClientClusterTests, ClusterConnect)
{
    // Really, since we get the cluster in the environment in SetUp, this
    // isn't really necessary.  That would raise an exception at startup if
    // it didn't connect, since we really want to cache the connection.
    // Just here for completness, and if things change.
    auto c = ClientTestEnvironment::get_cluster();
    ASSERT_TRUE(c.get() != nullptr);
}

TEST(SimpleClientClusterTests, ClusterCopy)
{
    auto c = ClientTestEnvironment::get_cluster();
    cluster copy = *c;
    ASSERT_FALSE(*c == copy);
}

TEST(SimpleClientClusterTests, CanGetBucket)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    ASSERT_TRUE(b.get() != nullptr);
}

TEST(SimpleClientClusterTests, CanGetBucket_NoBucket)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto random_bucket = ClientTestEnvironment::get_uuid();
    ASSERT_THROW(c->bucket(random_bucket), std::runtime_error);
}

TEST(SimpleClientClusterTests, CanOpenMultipleBuckets)
{
    // NOTE - this _assumes_ secBucket exists.  The fit
    // performer insists on this, so seems ok.  Will insure
    // jenkins job puts it in too.
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    auto b2 = c->bucket("secBucket");
}

TEST(SimpleClientClusterTests, CanListBucketNames)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto buckets = c->buckets();
    ASSERT_TRUE(std::find(buckets.begin(), buckets.end(), std::string("default")) != buckets.end());
}

TEST(SimpleClientClusterTests, CachesBuckets)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto b1 = c->bucket("default");
    auto b2 = c->bucket("default");
    ASSERT_EQ((void*)&(*b1), (void*)&(*b2));
}

TEST(SimpleClientClusterTests, CreateDestroyEvents)
{
    instance_pool_event_counter ev;
    auto conf = ClientTestEnvironment::get_conf();
    {
        auto c =
          std::make_shared<cluster>(conf["connection_string"], conf["username"], conf["password"], cluster_options().event_counter(&ev));
        auto b = c->bucket("default");
    }
    ASSERT_EQ(1, ev.cluster_counter.create.load());
    ASSERT_EQ(0, ev.cluster_counter.destroy.load());
    ASSERT_EQ(1, ev.cluster_counter.remove.load());
    ASSERT_EQ(0, ev.cluster_counter.destroy_not_available.load());
    ASSERT_EQ(0, ev.bucket_counters["default"].create.load());
    ASSERT_EQ(1, ev.bucket_counters["default"].destroy.load());
    ASSERT_EQ(0, ev.bucket_counters["default"].destroy_not_available.load());
}

TEST(SimpleClientClusterTests, CanGetSetKVTimeout)
{
    std::chrono::microseconds our_timeout(12345);
    auto conf = ClientTestEnvironment::get_conf();
    auto c =
      std::make_shared<cluster>(conf["connection_string"], conf["username"], conf["password"], cluster_options().kv_timeout(our_timeout));
    // be sure this matches what we set.  When testing cluster, we verify that the timeout is
    // actually honored
    ASSERT_EQ(our_timeout, c->default_kv_timeout());
}

TEST(SimpleClientBucketTests, CanGetDefaultCollection)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    auto coll = b->default_collection();
    ASSERT_TRUE(coll.get() != nullptr);
}

TEST(SimpleClientBucketTests, CanSpecifyScopeAndCollection)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    auto coll = b->collection("ascope.acollection");
    ASSERT_EQ(coll->scope(), "ascope");
    ASSERT_EQ(coll->name(), "acollection");
}

TEST(SimpleClientBucketTests, CanSpecifyJustCollection)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    auto coll = b->collection("acollection");
    ASSERT_EQ(coll->scope(), "_default");
    ASSERT_EQ(coll->name(), "acollection");
}

TEST(SimpleClientBucketTests, CanSpecifyEmptyScope)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    auto coll = b->collection(".acollection");
    ASSERT_EQ(coll->scope(), "_default");
    ASSERT_EQ(coll->name(), "acollection");
}

TEST(SimpleClientBucketTests, CanSpecifyEmptyCollection)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    auto coll = b->collection("ascope.");
    ASSERT_EQ(coll->scope(), "ascope");
    ASSERT_EQ(coll->name(), "_default");
}

TEST(SimpleClientBucketTests, CanNotSpecifyEmptyStringForCollection)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    EXPECT_THROW(b->collection(""), std::runtime_error);
}

TEST(SimpleClientBucketTests, CanGetBucketName)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    ASSERT_EQ(b->name(), "default");
}

TEST(SimpleClientBucketTests, CachesCollections)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    auto coll1 = b->collection("somecollection");
    auto coll2 = b->collection("somecollection");
    ASSERT_EQ((void*)&(*coll1), (void*)&(*coll2));
}

TEST(SimpleClientBucketTests, CanGetKVTimeout)
{
    auto conf = ClientTestEnvironment::get_conf();
    std::chrono::microseconds our_default(1234);
    auto c =
      std::make_shared<cluster>(conf["connection_string"], conf["username"], conf["password"], cluster_options().kv_timeout(our_default));
    ASSERT_EQ(c->default_kv_timeout(), our_default);
    auto b = c->bucket("default");
    ASSERT_EQ(b->default_kv_timeout(), our_default);
}

class SimpleClientCollectionTests : public ::testing::Test
{

  protected:
    void SetUp() override
    {
        // no need to ask for the bucket or cluster if we already have 'em
        _coll = ClientTestEnvironment::get_cluster()->bucket("default")->default_collection();

        // new id every time
        _id = ClientTestEnvironment::get_uuid();

        auto result = _coll->upsert(_id, content);
        if (result.rc == 0) {
            spdlog::info("successfully upserted, got {}", result);
            return;
        }
        FAIL() << "couldn't upsert into bucket - " << result.strerror();
    }

    void TearDown() override
    {
        spdlog::info("tearing down, removing {}", _id);
        if (nullptr != _coll) {
            _coll->remove(_id);
        }
    }

    std::shared_ptr<couchbase::collection> _coll;
    std::string _id;
};

TEST_F(SimpleClientCollectionTests, CanExists)
{
    auto res = _coll->exists(_id);
    ASSERT_TRUE(res.is_success());
    ASSERT_TRUE(res.content_as<bool>());
    ASSERT_TRUE(res.cas > 0);
}

TEST_F(SimpleClientCollectionTests, ExistsWhenNotExists)
{
    auto id = ClientTestEnvironment::get_uuid();
    auto res = _coll->exists(id);
    ASSERT_FALSE(res.content_as<bool>());
}

TEST_F(SimpleClientCollectionTests, ExistsWhenDeleted)
{
    auto r0 = _coll->remove(_id);
    ASSERT_TRUE(r0.is_success());
    auto r1 = _coll->exists(_id);
    ASSERT_FALSE(r1.content_as<bool>());
}

TEST_F(SimpleClientCollectionTests, CanInsert)
{
    auto id = ClientTestEnvironment::get_uuid();
    auto c = nlohmann::json::parse("{\"some\":\"thing\"}");
    auto result = _coll->insert(id, content);
    ASSERT_TRUE(result.is_success());
    ASSERT_EQ(result.rc, 0);
    ASSERT_FALSE(result.is_not_found());
    ASSERT_FALSE(result.is_value_too_large());
    ASSERT_TRUE(result.strerror().find("LCB_SUCCESS") != std::string::npos);
    ASSERT_EQ(result.key, id);
    ASSERT_FALSE(result.has_value());
    ASSERT_FALSE(result.is_deleted);
}

TEST_F(SimpleClientCollectionTests, CanUpsert)
{
    std::string id;
    upsert_random_doc(_coll, id);
}

TEST_F(SimpleClientCollectionTests, CanUpsertRawString)
{
    auto id = ClientTestEnvironment::get_uuid();
    std::string c = "\"{\"some\":\"object\", \"parsed\":\"externally\"}\"";
    auto res = _coll->upsert(id, c);
    ASSERT_TRUE(res.is_success());
    ASSERT_EQ(c, _coll->get(id).content_as<std::string>());
}

TEST_F(SimpleClientCollectionTests, CanGet)
{
    // Of course, this depends on being able to upsert as well
    auto get_res = _coll->get(_id);
    ASSERT_TRUE(get_res.is_success());
    ASSERT_FALSE(get_res.is_not_found());
    ASSERT_FALSE(get_res.is_value_too_large());
    ASSERT_TRUE(get_res.strerror().find("LCB_SUCCESS") != std::string::npos);
    ASSERT_NE(get_res.cas, 0);
    ASSERT_EQ(get_res.key, _id);
    ASSERT_EQ(get_res.rc, 0);
    ASSERT_TRUE(get_res.has_value());
    ASSERT_EQ(get_res.content_as<nlohmann::json>(), content);
    ASSERT_FALSE(get_res.is_deleted);
}

TEST_F(SimpleClientCollectionTests, CanGetIntoObject)
{
    SimpleObject o{ "foo", 100 };
    auto id = ClientTestEnvironment::get_uuid();
    auto res = _coll->upsert(id, o);
    ASSERT_TRUE(res.is_success());
    ASSERT_EQ(o, _coll->get(id).content_as<SimpleObject>());
}

TEST_F(SimpleClientCollectionTests, ContentAsCanGetObjectAsStringOrNLohmannJson)
{
    SimpleObject o{ "foo", 100 };
    nlohmann::json j = o;
    auto id = ClientTestEnvironment::get_uuid();
    ASSERT_TRUE(_coll->upsert(id, o).is_success());
    auto res = _coll->get(id);
    auto s = res.content_as<std::string>();
    ASSERT_EQ(j, nlohmann::json::parse(res.content_as<std::string>()));
    ASSERT_EQ(j, res.content_as<nlohmann::json>());
}

TEST_F(SimpleClientCollectionTests, CanGetDocNotFound)
{
    auto id = ClientTestEnvironment::get_uuid();
    auto res = _coll->get(id);
    ASSERT_FALSE(res.is_success());
    ASSERT_TRUE(res.is_not_found());
    ASSERT_FALSE(res.is_value_too_large());
    ASSERT_TRUE(res.strerror().find("LCB_SUCCESS") == std::string::npos);
    ASSERT_TRUE(res.key.empty());
    ASSERT_FALSE(res.is_deleted);
    ASSERT_EQ(res.rc, 301);
}

TEST_F(SimpleClientCollectionTests, CanRemove)
{
    // Of course, this depends on being able to upsert as well
    auto res = _coll->remove(_id);
    ASSERT_TRUE(res.is_success());
    res = _coll->get(_id);
    ASSERT_FALSE(res.is_success());
    ASSERT_TRUE(res.is_not_found());
    ASSERT_FALSE(res.is_value_too_large());
    ASSERT_TRUE(res.key.empty());
    ASSERT_EQ(res.rc, 301);
    ASSERT_FALSE(res.is_deleted);
}

TEST_F(SimpleClientCollectionTests, CanReplace)
{
    // Of course, this depends on being able to upsert and get.
    auto cas = _coll->get(_id).cas;
    auto new_content = nlohmann::json::parse("{\"some\":\"thing else\"}");
    auto res = _coll->replace(_id, new_content, replace_options().cas(cas));
    ASSERT_TRUE(res.is_success());
    res = _coll->get(_id);
    ASSERT_TRUE(res.is_success());
    ASSERT_NE(res.cas, cas);
    ASSERT_EQ(res.content_as<nlohmann::json>(), new_content);
    ASSERT_FALSE(res.is_deleted);
}

TEST_F(SimpleClientCollectionTests, CanReplaceFailCASMismatch)
{
    // Of course, this depends on being able to upsert and get.
    auto cas = _coll->get(_id).cas;
    auto new_content = nlohmann::json::parse("{\"some\":\"thing else\"}");
    auto res = _coll->replace(_id, new_content, replace_options().cas(100));
    ASSERT_FALSE(res.is_success());
    res = _coll->get(_id);
    ASSERT_TRUE(res.is_success());
    ASSERT_EQ(res.cas, cas);
    ASSERT_EQ(res.content_as<nlohmann::json>(), content);
    ASSERT_FALSE(res.is_deleted);
}

TEST_F(SimpleClientCollectionTests, CanLookupIn)
{
    // also depends on upsert
    auto res = _coll->lookup_in(_id, { couchbase::lookup_in_spec::get("some"), lookup_in_spec::fulldoc_get() });
    ASSERT_TRUE(res.is_success());
    ASSERT_FALSE(res.is_not_found());
    ASSERT_FALSE(res.is_value_too_large());
    ASSERT_EQ(res.key, _id);
    ASSERT_FALSE(res.has_value());
    ASSERT_FALSE(res.values.empty());
    ASSERT_EQ(res.values[0].content_as<std::string>(), std::string("thing"));
    ASSERT_EQ(res.values[0].status, LCB_SUCCESS);
    ASSERT_EQ(res.values[1].content_as<nlohmann::json>(), ::content);
    ASSERT_EQ(res.values[1].status, LCB_SUCCESS);
    ASSERT_FALSE(res.is_deleted);
}

TEST_F(SimpleClientCollectionTests, CanMutateIn)
{
    // also depends on upsert and get.
    auto res = _coll->mutate_in(_id,
                                { couchbase::mutate_in_spec::upsert(std::string("some"), std::string("other thing")),
                                  couchbase::mutate_in_spec::insert(std::string("another"), std::string("field")) });
    ASSERT_TRUE(res.is_success());
    res = _coll->get(_id);
    ASSERT_TRUE(res.is_success());
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.content_as<nlohmann::json>(), nlohmann::json::parse("{\"some\":\"other thing\", \"another\":\"field\"}"));
    ASSERT_FALSE(res.is_deleted);
}

TEST_F(SimpleClientCollectionTests, CanGetBucketNameEtc)
{
    ASSERT_EQ(_coll->bucket_name(), ::bucket_name);
    ASSERT_EQ(std::string("_default"), _coll->name());
    ASSERT_EQ(std::string("_default"), _coll->scope());
}

TEST_F(SimpleClientCollectionTests, CanGetKVTimeout)
{
    auto conf = ClientTestEnvironment::get_conf();
    std::chrono::microseconds our_default(1234);
    auto c =
      std::make_shared<cluster>(conf["connection_string"], conf["username"], conf["password"], cluster_options().kv_timeout(our_default));
    ASSERT_EQ(c->default_kv_timeout(), our_default);
    auto coll = c->bucket("default")->default_collection();
    ASSERT_EQ(coll->default_kv_timeout(), our_default);
}

TEST_F(SimpleClientCollectionTests, CanNotGetInCollectionThatDoesNotExist)
{
    if (ClientTestEnvironment::supports_collections()) {
        auto cluster = ClientTestEnvironment::get_cluster();
        auto bad_coll = cluster->bucket("default")->collection("idonotexist");
        auto r = bad_coll->get(_id);
        ASSERT_TRUE(r.is_timeout());
    }
}

TEST_F(SimpleClientCollectionTests, CanNotMutateInCollectionThatDoesNotExist)
{
    if (ClientTestEnvironment::supports_collections()) {
        auto cluster = ClientTestEnvironment::get_cluster();
        auto bad_coll = cluster->bucket("default")->collection("idonotexist");
        auto r = bad_coll->upsert(_id, content);
        ASSERT_TRUE(r.is_timeout());
    }
}

TEST_F(SimpleClientCollectionTests, CanNotMutateInScopeThatDoesNotExist)
{
    if (ClientTestEnvironment::supports_collections()) {
        auto cluster = ClientTestEnvironment::get_cluster();
        auto bad_coll = cluster->bucket("default")->collection("idonotexist.");
        auto r = bad_coll->upsert(_id, content);
        ASSERT_TRUE(r.is_timeout());
    }
}

TEST_F(SimpleClientCollectionTests, CanNotMutateInScopeAndCollectionThatDoesNotExist)
{
    if (ClientTestEnvironment::supports_collections()) {
        auto cluster = ClientTestEnvironment::get_cluster();
        auto bad_coll = cluster->bucket("default")->collection("idonotexist.neitherdoi");
        auto r = bad_coll->upsert(_id, content);
        ASSERT_TRUE(r.is_timeout());
    }
}

TEST_F(SimpleClientCollectionTests, CanDurableRemove)
{
    auto res = _coll->remove(_id, remove_options().durability(durability_level::majority));
    ASSERT_TRUE(res.is_success());
}

int
main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new ClientTestEnvironment());
    return RUN_ALL_TESTS();
}
