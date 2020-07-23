#include <memory>
#include <gtest/gtest.h>
#include "couchbase/client/cluster.hxx"
#include "couchbase/client/collection.hxx"
#include "client_env.h"

using namespace couchbase;

static std::string bucket_name = std::string("default");
static nlohmann::json content = nlohmann::json::parse("{\"some\":\"thing\"}");

static void upsert_random_doc(std::shared_ptr<couchbase::collection>& coll, std::string& id) {
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

TEST(SimpleClientClusterTests, ClusterConnect) {
    // Really, since we get the cluster in the environment in SetUp, this
    // isn't really necessary.  That would raise an exception at startup if
    // it didn't connect, since we really want to cache the connection.
    // Just here for completness, and if things change.
    auto c = ClientTestEnvironment::get_cluster();
    ASSERT_TRUE(c.get() != nullptr);
}

TEST(SimpleClientClusterTests, CanGetBucket) {
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    ASSERT_TRUE(b.get() != nullptr);
}

TEST(SimpleClientClusterTests, CanGetBucket_NoBucket) {
    auto c = ClientTestEnvironment::get_cluster();
    auto random_bucket = ClientTestEnvironment::get_uuid();
    ASSERT_THROW(c->bucket(random_bucket), std::runtime_error);
}

TEST(SimpleClientClusterTests, CanListBucketNames) {
    auto c = ClientTestEnvironment::get_cluster();
    auto buckets = c->buckets();
    ASSERT_TRUE(std::find(buckets.begin(), buckets.end(), std::string("default")) != buckets.end());
}

TEST(SimpleClientBucketTests, CanGetDefaultCollection) {
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    auto coll = b->default_collection();
    ASSERT_TRUE(coll.get() != nullptr);
}

TEST(SimpleClientCollectionTests, CanInsert) {
    auto id = ClientTestEnvironment::get_uuid();
    auto content = nlohmann::json::parse("{\"some\":\"thing\"}");
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    auto coll = b->default_collection();
    auto result = coll->insert(id, content);
    ASSERT_TRUE(result.is_success());
    ASSERT_EQ(result.rc, 0);
    ASSERT_FALSE(result.is_not_found());
    ASSERT_FALSE(result.is_value_too_large());
    ASSERT_TRUE(result.strerror().find("LCB_SUCCESS") != std::string::npos);
    ASSERT_EQ(result.key, id);
    ASSERT_FALSE(result.value);
}

TEST(SimpleClientCollectionTests, CanUpsert) {
    std::string id;
    auto coll = ClientTestEnvironment::get_default_collection(::bucket_name);
    upsert_random_doc(coll, id);
}

TEST(SimpleClientCollectionTests, CanGet) {
    // Of course, this depends on being able to upsert as well
    std::string id;
    auto coll = ClientTestEnvironment::get_default_collection(::bucket_name);
    upsert_random_doc(coll, id);
    auto get_res = coll->get(id);
    ASSERT_TRUE(get_res.is_success());
    ASSERT_FALSE(get_res.is_not_found());
    ASSERT_FALSE(get_res.is_value_too_large());
    ASSERT_TRUE(get_res.strerror().find("LCB_SUCCESS") != std::string::npos);
    ASSERT_NE(get_res.cas, 0);
    ASSERT_EQ(get_res.key, id);
    ASSERT_EQ(get_res.rc, 0);
    ASSERT_TRUE(get_res.value);
    ASSERT_EQ(get_res.value.get(), content);
}

TEST(SimpleClientCollectionTests, CanGetDocNotFound) {
    auto id = ClientTestEnvironment::get_uuid();
    auto c = ClientTestEnvironment::get_cluster();
    auto b = c->bucket("default");
    auto coll = b->default_collection();
    auto res = coll->get(id);
    ASSERT_FALSE(res.is_success());
    ASSERT_TRUE(res.is_not_found());
    ASSERT_FALSE(res.is_value_too_large());
    ASSERT_TRUE(res.strerror().find("LCB_SUCCESS") == std::string::npos);
    ASSERT_TRUE(res.key.empty());
    ASSERT_EQ(res.rc, 301);
}

TEST(SimpleClientCollectionTests, CanRemove) {
    // Of course, this depends on being able to upsert as well
    std::string id;
    auto coll = ClientTestEnvironment::get_default_collection(::bucket_name);
    upsert_random_doc(coll, id);
    auto res = coll->remove(id);
    ASSERT_TRUE(res.is_success());
    res = coll->get(id);
    ASSERT_FALSE(res.is_success());
    ASSERT_TRUE(res.is_not_found());
    ASSERT_FALSE(res.is_value_too_large());
    ASSERT_TRUE(res.key.empty());
    ASSERT_EQ(res.rc, 301);
}

TEST(SimpleClientCollectionTests, CanReplace) {
    // Of course, this depends on being able to upsert and get.
    std::string id;
    auto coll = ClientTestEnvironment::get_default_collection(::bucket_name);
    upsert_random_doc(coll, id);
    auto cas = coll->get(id).cas;
    auto new_content = nlohmann::json::parse("{\"some\":\"thing else\"}");
    auto res = coll->replace(id, new_content, cas);
    ASSERT_TRUE(res.is_success());
    res = coll->get(id);
    ASSERT_TRUE(res.is_success());
    ASSERT_NE(res.cas, cas);
    ASSERT_EQ(res.value->get<nlohmann::json>(), new_content);
}

TEST(SimpleClientCollectionTests, CanLookupIn) {
    // also depends on upsert
    std::string id;
    auto coll = ClientTestEnvironment::get_default_collection(::bucket_name);
    upsert_random_doc(coll, id);
    auto res = coll->lookup_in(id, {couchbase::lookup_in_spec::get("some"), lookup_in_spec::fulldoc_get()});
    ASSERT_TRUE(res.is_success());
    ASSERT_FALSE(res.is_not_found());
    ASSERT_FALSE(res.is_value_too_large());
    ASSERT_EQ(res.key, id);
    ASSERT_FALSE(res.value);
    ASSERT_FALSE(res.values.empty());
    ASSERT_EQ(res.values[0]->get<std::string>(), std::string("thing"));
    ASSERT_EQ(res.values[1]->get<nlohmann::json>(), ::content);
}

TEST(SimpleClientCollectionTests, CanMutateIn) {
    // also depends on upsert and get.
    std::string id;
    auto coll = ClientTestEnvironment::get_default_collection(::bucket_name);
    upsert_random_doc(coll, id);
    auto res = coll->mutate_in(id, {couchbase::mutate_in_spec::upsert(std::string("some"), std::string("other thing")),
                                    couchbase::mutate_in_spec::insert(std::string("another"), std::string("field"))});
    ASSERT_TRUE(res.is_success());
    res = coll->get(id);
    ASSERT_TRUE(res.is_success());
    ASSERT_TRUE(res.value);
    ASSERT_EQ(res.value->get<nlohmann::json>(), nlohmann::json::parse("{\"some\":\"other thing\", \"another\":\"field\"}"));
}

TEST(SimpleClientCollectionTests, CanGetBucketNameEtc) {
    auto coll = ClientTestEnvironment::get_default_collection(::bucket_name);
    ASSERT_EQ(coll->bucket_name(), ::bucket_name);
    ASSERT_TRUE(coll->name().empty());
    ASSERT_TRUE(coll->scope().empty());
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new ClientTestEnvironment());
    return RUN_ALL_TESTS();
}
