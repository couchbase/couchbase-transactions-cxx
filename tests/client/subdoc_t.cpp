#include "client_env.h"
#include "couchbase/client/cluster.hxx"
#include "couchbase/client/collection.hxx"
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

using namespace couchbase;

static std::string bucket_name = std::string("default");
static nlohmann::json content = nlohmann::json::parse("{\"a\":\"string\", \"b\":\"other string\"}");

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

TEST(MutateInTests, CanUpsert)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    upsert_random_doc(coll, id);
    auto r1 = coll->mutate_in(id, { mutate_in_spec::upsert("a", "more strings") });
    ASSERT_EQ(0, r1.rc);
    auto r2 = coll->get(id);
    ASSERT_EQ(r2.value->get<nlohmann::json>(), nlohmann::json::parse("{\"a\":\"more strings\", \"b\":\"other string\"}"));
}

TEST(MutateInTests, CanUpsertXAttr)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    upsert_random_doc(coll, id);
    auto r1 = coll->mutate_in(id, { mutate_in_spec::upsert("aaa", "more strings").xattr() });
    ASSERT_EQ(0, r1.rc);
    auto r2 = coll->lookup_in(id, { lookup_in_spec::get("aaa").xattr(), lookup_in_spec::fulldoc_get() });
    ASSERT_EQ(r2.values[0]->get<std::string>(), std::string("more strings"));
    ASSERT_EQ(r2.values[1]->get<nlohmann::json>(), content);
}

TEST(MutateInTests, CanInsert)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    upsert_random_doc(coll, id);
    auto r1 = coll->mutate_in(id, { mutate_in_spec::insert("c", "more strings") });
    ASSERT_EQ(0, r1.rc);
    auto r2 = coll->get(id);
    ASSERT_EQ(r2.value->get<nlohmann::json>(), nlohmann::json::parse("{\"a\":\"string\", \"b\":\"other string\",\"c\":\"more strings\"}"));
}

TEST(MutateInTests, CanInsertXAttr)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    upsert_random_doc(coll, id);
    auto r1 = coll->mutate_in(id, { mutate_in_spec::insert("aaa", "more strings").xattr() });
    ASSERT_EQ(0, r1.rc);
    auto r2 = coll->lookup_in(id, { lookup_in_spec::get("aaa").xattr(), lookup_in_spec::fulldoc_get() });
    ASSERT_EQ(r2.values[0]->get<std::string>(), std::string("more strings"));
    ASSERT_EQ(r2.values[1]->get<nlohmann::json>(), content);
}

TEST(MutateInTests, CanRemove)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    upsert_random_doc(coll, id);
    auto r1 = coll->mutate_in(id, { mutate_in_spec::remove("a") });
    ASSERT_EQ(0, r1.rc);
    auto r2 = coll->get(id);
    ASSERT_EQ(0, r2.rc);
    ASSERT_EQ(r2.value->get<nlohmann::json>(), nlohmann::json::parse("{\"b\":\"other string\"}"));
}

TEST(MutateInTests, CanRemoveXAttr)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    upsert_random_doc(coll, id);
    auto r0 = coll->mutate_in(id, { mutate_in_spec::upsert("aaa", "more strings").xattr() });
    ASSERT_EQ(0, r0.rc);
    auto r1 = coll->lookup_in(id, { lookup_in_spec::get("aaa").xattr(), lookup_in_spec::fulldoc_get() });
    ASSERT_EQ(r1.values[0]->get<std::string>(), std::string("more strings"));
    ASSERT_EQ(r1.values[1]->get<nlohmann::json>(), content);
    auto r2 = coll->mutate_in(id, { mutate_in_spec::remove("aaa").xattr() });
    ASSERT_EQ(0, r2.rc);
    auto r3 = coll->lookup_in(id, { lookup_in_spec::get("aaa").xattr(), lookup_in_spec::fulldoc_get() });
    ASSERT_EQ(0, r3.rc);
    ASSERT_FALSE(r3.values[0]);
    ASSERT_EQ(r3.values[1]->get<nlohmann::json>(), content);
}

TEST(MutateInTests, CanFullDocUpsert)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    upsert_random_doc(coll, id);
    auto new_content = nlohmann::json::parse("{\"I\":\"am completely different\"}");
    auto r1 = coll->mutate_in(id, { mutate_in_spec::fulldoc_upsert(new_content) });
    ASSERT_EQ(0, r1.rc);
    auto r2 = coll->get(id);
    ASSERT_EQ(0, r2.rc);
    ASSERT_EQ(r2.value->get<nlohmann::json>(), new_content);
}

TEST(MutateInTests, CanFullDocInsert)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    auto new_content = nlohmann::json::parse("{\"I\":\"am completely new\"}");
    auto r0 = coll->get(id);
    ASSERT_TRUE(r0.is_not_found());
    // note - you can't do a 'naked' fulldoc_insert
    auto r1 = coll->mutate_in(id, { mutate_in_spec::insert("a", "string").xattr(), mutate_in_spec::fulldoc_insert(new_content) });
    ASSERT_EQ(0, r1.rc);
    auto r2 = coll->get(id);
    ASSERT_EQ(0, r2.rc);
    ASSERT_EQ(r2.value->get<nlohmann::json>(), new_content);
}

TEST(MutateInTests, CanMutateIfCorrectCas)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    upsert_random_doc(coll, id);
    auto r0 = coll->get(id);
    ASSERT_TRUE(r0.is_success());
    auto cas = r0.cas;
    auto r1 = coll->mutate_in(id, { mutate_in_spec::upsert("a", "string").xattr() }, mutate_in_options().cas(cas));
    ASSERT_TRUE(r1.is_success());
    ASSERT_TRUE(r1.cas != cas);
    auto r2 = coll->lookup_in(id, { lookup_in_spec::get("a").xattr() });
    ASSERT_TRUE(r1.is_success());
    ASSERT_EQ(std::string("string"), r2.values[0]->get<std::string>());
}

TEST(MutateInTests, CanNotMutateIfIncorrectCas)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    upsert_random_doc(coll, id);
    auto r1 = coll->mutate_in(id, { mutate_in_spec::upsert("a", "string").xattr() }, mutate_in_options().cas(100));
    ASSERT_FALSE(r1.is_success());
}

TEST(MutateInTests, CanMutateMultipleSpecsWithPathsXattr)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    upsert_random_doc(coll, id);
    auto r0 = coll->mutate_in(id,
                              { mutate_in_spec::insert("a.x", "x").create_path().xattr(),
                                mutate_in_spec::insert("a.y", "y").xattr(),
                                mutate_in_spec::insert("a.z", "z").xattr() });
    ASSERT_EQ(0, r0.rc);
    auto r1 = coll->lookup_in(id, { lookup_in_spec::get("a").xattr(), lookup_in_spec::get("a.x").xattr() });
    ASSERT_EQ(0, r1.rc);
    ASSERT_EQ(r1.values[0]->get<nlohmann::json>(), nlohmann::json::parse("{\"x\":\"x\",\"y\":\"y\",\"z\":\"z\"}"));
}

TEST(MutateInTests, CanCreateAsDeleted)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    auto r0 = coll->mutate_in(id,
                              { mutate_in_spec::insert("a.x", "x").create_path().xattr(),
                                mutate_in_spec::insert("a.y", "y").xattr(),
                                mutate_in_spec::insert("a.z", "z").xattr() },
                              mutate_in_options().create_as_deleted(true));
    ASSERT_EQ(0, r0.rc);
    auto r1 = coll->get(id);
    ASSERT_TRUE(r1.is_not_found());
}

TEST(MutateInTests, CanAccessDeleted)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    auto r0 = coll->mutate_in(id,
                              { mutate_in_spec::insert("a.x", "x").create_path().xattr(),
                                mutate_in_spec::insert("a.y", "y").xattr(),
                                mutate_in_spec::insert("a.z", "z").xattr() },
                              mutate_in_options().create_as_deleted(true));
    ASSERT_EQ(0, r0.rc);

    auto r1 = coll->mutate_in(id,
                              {
                                mutate_in_spec::upsert("a.zz", "zz").xattr(),
                              },
                              mutate_in_options().access_deleted(true));
    ASSERT_TRUE(r1.is_success());
    auto r2 = coll->lookup_in(id, { lookup_in_spec::get("a.zz").xattr() }, lookup_in_options().access_deleted(true));
    ASSERT_TRUE(r2.is_success());
    ASSERT_EQ(r2.values[0]->get<std::string>(), std::string("zz"));
}

TEST(MutateInTests, CanMutateCreateAsDeletedWithCas)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    auto r0 = coll->mutate_in(id,
                              { mutate_in_spec::insert("a.x", "x").create_path().xattr(),
                                mutate_in_spec::insert("a.y", "y").xattr(),
                                mutate_in_spec::insert("a.z", "z").xattr() },
                              mutate_in_options().create_as_deleted(true));
    ASSERT_EQ(0, r0.rc);

    auto r1 = coll->mutate_in(
      id, { mutate_in_spec::upsert("a.zz", "zz").xattr() }, mutate_in_options().access_deleted(true).create_as_deleted(true));
    ASSERT_FALSE(r1.is_success());
    ASSERT_EQ(r1.rc, 305);

    auto r2 = coll->mutate_in(
      id, { mutate_in_spec::upsert("a.zz", "zz").xattr() }, mutate_in_options().cas(r0.cas).access_deleted(true).create_as_deleted(true));
    ASSERT_TRUE(r2.is_success());
    auto r3 = coll->lookup_in(id, { lookup_in_spec::get("a.zz").xattr() }, lookup_in_options().access_deleted(true));
    ASSERT_EQ("zz", r3.values[0]->get<std::string>());
    ASSERT_TRUE(r3.cas != 0);
    ASSERT_TRUE(r3.cas != r0.cas);
}

TEST(MutateInTests, CanNotMutateCreateAsDeletedWithWrongCas)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    auto r0 = coll->mutate_in(id,
                              { mutate_in_spec::insert("a.x", "x").create_path().xattr(),
                                mutate_in_spec::insert("a.y", "y").xattr(),
                                mutate_in_spec::insert("a.z", "z").xattr() },
                              mutate_in_options().create_as_deleted(true));
    ASSERT_EQ(0, r0.rc);

    auto r1 = coll->mutate_in(
      id, { mutate_in_spec::upsert("a.zz", "zz").xattr() }, mutate_in_options().cas(100).access_deleted(true).create_as_deleted(true));
    ASSERT_FALSE(r1.is_success());
    ASSERT_EQ(r1.rc, 305);
}

TEST(MutateInTests, CanNotAccessDeleted)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    auto r0 = coll->mutate_in(id,
                              { mutate_in_spec::insert("a.x", "x").create_path().xattr(),
                                mutate_in_spec::insert("a.y", "y").xattr(),
                                mutate_in_spec::insert("a.z", "z").xattr() },
                              mutate_in_options().create_as_deleted(true));
    ASSERT_EQ(0, r0.rc);

    auto r1 = coll->mutate_in(id, { mutate_in_spec::upsert("a.zz", "zz").xattr() });
    ASSERT_TRUE(r1.is_not_found());
}

TEST(MutateInTests, AccessDeleteOkWhenNotDeleted)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    upsert_random_doc(coll, id);
    auto r0 = coll->mutate_in(id,
                              { mutate_in_spec::insert("a.x", "x").create_path().xattr(),
                                mutate_in_spec::insert("a.y", "y").xattr(),
                                mutate_in_spec::insert("a.z", "z").xattr() },
                              mutate_in_options().access_deleted(true));
    ASSERT_TRUE(r0.is_success());
    auto r1 = coll->lookup_in(id, { lookup_in_spec::get("a").xattr() });
    ASSERT_TRUE(r1.is_success());
    ASSERT_EQ(r1.values[0]->get<nlohmann::json>(), nlohmann::json::parse("{\"x\":\"x\",\"y\":\"y\",\"z\":\"z\"}"));
}

TEST(LookupInTests, CanNotAccessDeleted)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    auto r0 = coll->mutate_in(id,
                              { mutate_in_spec::insert("a.x", "x").create_path().xattr(),
                                mutate_in_spec::insert("a.y", "y").xattr(),
                                mutate_in_spec::insert("a.z", "z").xattr() },
                              mutate_in_options().create_as_deleted(true));
    ASSERT_EQ(0, r0.rc);

    auto r1 = coll->lookup_in(id,
                              {
                                lookup_in_spec::get("a").xattr(),
                              },
                              lookup_in_options().access_deleted(false));
    ASSERT_TRUE(r1.is_not_found());
}

TEST(LookupInTests, CanAccessDeleted)
{
    auto c = ClientTestEnvironment::get_cluster();
    auto coll = c->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    auto r0 = coll->mutate_in(id,
                              { mutate_in_spec::insert("a.x", "x").create_path().xattr(),
                                mutate_in_spec::insert("a.y", "y").xattr(),
                                mutate_in_spec::insert("a.z", "z").xattr() },
                              mutate_in_options().create_as_deleted(true));
    ASSERT_EQ(0, r0.rc);

    auto r1 = coll->lookup_in(id, { lookup_in_spec::get("a").xattr() }, lookup_in_options().access_deleted(true));
    ASSERT_EQ(0, r1.rc);
    ASSERT_EQ(r1.values[0]->get<nlohmann::json>(), nlohmann::json::parse("{\"x\":\"x\",\"y\":\"y\",\"z\":\"z\"}"));
    ASSERT_TRUE(r1.is_deleted);
}
