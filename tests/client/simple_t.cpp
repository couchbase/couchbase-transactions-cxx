#include <chrono>
#include <cstdio>
#include <memory>
#include <thread>

#include "client_env.h"
#include "couchbase/client/cluster.hxx"
#include "couchbase/client/collection.hxx"
#include <gtest/gtest.h>
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
    ASSERT_FALSE(result.value);
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

TEST(SimpleClientClusterTests, CanListBucketNames)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto buckets = c->buckets();
    ASSERT_TRUE(std::find(buckets.begin(), buckets.end(), std::string("default")) != buckets.end());
}

TEST(SimpleClientBucketTests, CanGetDefaultCollection)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    auto coll = b->default_collection();
    ASSERT_TRUE(coll.get() != nullptr);
}

class SimpleClientCollectionTests : public ::testing::Test
{

  protected:
    void SetUp() override
    {
        // no need to ask for the bucket or cluster if we already have 'em
        if (!_bucket) {
            spdlog::info("asking for default bucket, and default collection");
            _bucket = ClientTestEnvironment::get_cluster()->bucket("default");
            _coll = _bucket->default_collection();

            // TODO: This is a miserable hack.
            spdlog::info("Waiting until ready by sleeping {} seconds...");
            std::this_thread::sleep_for(std::chrono::seconds(10));
            spdlog::info("done sleeping");
        }
        // new id every time
        _id = ClientTestEnvironment::get_uuid();

        // for now, lets do this in hopes we avoid CCBC-1300
        int retries = 0;
        while (++retries < NUM_RETRIES) {
            spdlog::warn("attempting to upsert ({}) of {} attempts", retries, NUM_RETRIES);
            auto result = _coll->upsert(_id, content);
            if (result.rc == 0) {
                spdlog::info("successfully upserted, got {}", result);
                return;
            } else if (result.rc != 201 && result.rc != 1040 && result.rc != 1031) {
                FAIL() << "got unexpected result code when upserting" << result.rc;
            } else {
                spdlog::info("upsert got {}, retrying in {} seconds", result, DELAY_SECONDS);
                std::this_thread::sleep_for(std::chrono::seconds(DELAY_SECONDS));
            }
        }
        FAIL() << "tried " << NUM_RETRIES << " times, waiting " << DELAY_SECONDS << "sec between, bucket still not ready";
    }

    void TearDown() override
    {
        spdlog::info("tearing down, removing {}", _id);
        _coll->remove(_id);
    }

    const int NUM_RETRIES{ 10 };
    const int DELAY_SECONDS{ 6 };
    static std::shared_ptr<couchbase::collection> _coll;
    static std::shared_ptr<couchbase::bucket> _bucket;
    std::string _id;
};
std::shared_ptr<couchbase::collection> SimpleClientCollectionTests::_coll = std::shared_ptr<couchbase::collection>();
std::shared_ptr<couchbase::bucket> SimpleClientCollectionTests::_bucket = std::shared_ptr<couchbase::bucket>();

TEST_F(SimpleClientCollectionTests, CanInsert)
{
    auto id = ClientTestEnvironment::get_uuid();
    auto content = nlohmann::json::parse("{\"some\":\"thing\"}");
    auto result = _coll->insert(id, content);
    ASSERT_TRUE(result.is_success());
    ASSERT_EQ(result.rc, 0);
    ASSERT_FALSE(result.is_not_found());
    ASSERT_FALSE(result.is_value_too_large());
    ASSERT_TRUE(result.strerror().find("LCB_SUCCESS") != std::string::npos);
    ASSERT_EQ(result.key, id);
    ASSERT_FALSE(result.value);
}

TEST_F(SimpleClientCollectionTests, CanUpsert)
{
    std::string id;
    upsert_random_doc(_coll, id);
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
    ASSERT_TRUE(get_res.value);
    ASSERT_EQ(get_res.value.get(), content);
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
}

TEST_F(SimpleClientCollectionTests, CanReplace)
{
    // Of course, this depends on being able to upsert and get.
    auto cas = _coll->get(_id).cas;
    auto new_content = nlohmann::json::parse("{\"some\":\"thing else\"}");
    auto res = _coll->replace(_id, new_content, cas);
    ASSERT_TRUE(res.is_success());
    res = _coll->get(_id);
    ASSERT_TRUE(res.is_success());
    ASSERT_NE(res.cas, cas);
    ASSERT_EQ(res.value->get<nlohmann::json>(), new_content);
}

TEST_F(SimpleClientCollectionTests, CanLookupIn)
{
    // also depends on upsert
    auto res = _coll->lookup_in(_id, { couchbase::lookup_in_spec::get("some"), lookup_in_spec::fulldoc_get() });
    ASSERT_TRUE(res.is_success());
    ASSERT_FALSE(res.is_not_found());
    ASSERT_FALSE(res.is_value_too_large());
    ASSERT_EQ(res.key, _id);
    ASSERT_FALSE(res.value);
    ASSERT_FALSE(res.values.empty());
    ASSERT_EQ(res.values[0]->get<std::string>(), std::string("thing"));
    ASSERT_EQ(res.values[1]->get<nlohmann::json>(), ::content);
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
    ASSERT_TRUE(res.value);
    ASSERT_EQ(res.value->get<nlohmann::json>(), nlohmann::json::parse("{\"some\":\"other thing\", \"another\":\"field\"}"));
}

TEST_F(SimpleClientCollectionTests, CanGetBucketNameEtc)
{
    ASSERT_EQ(_coll->bucket_name(), ::bucket_name);
    ASSERT_EQ(std::string("_default"), _coll->name());
    ASSERT_EQ(std::string("_default"), _coll->scope());
}

int
main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new ClientTestEnvironment());
    return RUN_ALL_TESTS();
}
