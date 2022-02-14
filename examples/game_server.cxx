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

#include <functional>
#include <iostream>
#include <random>
#include <string>

#include <couchbase/cluster.hxx>
#include <couchbase/transactions.hxx>
#include <spdlog/spdlog.h>

#include "couchbase/transactions/internal/utils.hxx"

using namespace std;
using namespace couchbase;

std::string
make_uuid()
{
    static std::random_device dev;
    static std::mt19937 rng(dev());

    uniform_int_distribution<int> dist(0, 15);

    const char* v = "0123456789abcdef";
    const bool dash[] = { 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0 };

    string res;
    for (int i = 0; i < 16; i++) {
        if (dash[i])
            res += "-";
        res += v[dist(rng)];
        res += v[dist(rng)];
    }
    return res;
}

struct Player {
    int experience;
    int hitpoints;
    std::string json_type;
    int level;
    bool logged_in;
    std::string name;
    std::string uuid;
};

void
to_json(nlohmann::json& j, const Player& p)
{
    /* clang-format off */
    j = nlohmann::json{ { "experience", p.experience },
                        { "hitpoints", p.hitpoints },
                        { "jsonType", p.json_type },
                        { "level", p.level },
                        { "loggedIn", p.logged_in },
                        { "name", p.name },
                        { "uuid", p.uuid } };
    /* clang-format on */
}

void
from_json(const nlohmann::json& j, Player& p)
{
    j.at("experience").get_to(p.experience);
    j.at("hitpoints").get_to(p.hitpoints);
    j.at("jsonType").get_to(p.json_type);
    j.at("level").get_to(p.level);
    j.at("loggedIn").get_to(p.logged_in);
    j.at("name").get_to(p.name);
    j.at("uuid").get_to(p.uuid);
}

struct Monster {
    int experience_when_killed;
    int hitpoints;
    double item_probability;
    std::string json_type;
    std::string name;
    std::string uuid;
};

void
to_json(nlohmann::json& j, const Monster& m)
{
    j = nlohmann::json{ { "experienceWhenKilled", m.experience_when_killed },
                        { "hitpoints", m.hitpoints },
                        { "itemProbability", m.item_probability },
                        { "jsonType", m.json_type },
                        { "name", m.name },
                        { "uuid", m.uuid } };
}

void
from_json(const nlohmann::json& j, Monster& m)
{
    j.at("experienceWhenKilled").get_to(m.experience_when_killed);
    j.at("hitpoints").get_to(m.hitpoints);
    j.at("itemProbability").get_to(m.item_probability);
    j.at("jsonType").get_to(m.json_type);
    j.at("name").get_to(m.name);
    j.at("uuid").get_to(m.uuid);
}
class GameServer
{
  private:
    transactions::transactions& transactions_;

  public:
    GameServer(transactions::transactions& transactions)
      : transactions_(transactions)
    {
    }

    CB_NODISCARD int calculate_level_for_experience(int experience) const
    {
        return experience / 100;
    }

    void player_hits_monster(const string&,
                             int damage_,
                             const couchbase::document_id& player_id,
                             const couchbase::document_id& monster_id,
                             atomic<bool>& exists)
    {
        try {
            transactions_.run([&](transactions::attempt_context& ctx) {
                auto monster = ctx.get_optional(monster_id);
                if (!monster) {
                    exists = false;
                    return;
                }
                const Monster& monster_body = monster->content<Monster>();

                int monster_hitpoints = monster_body.hitpoints;
                int monster_new_hitpoints = monster_hitpoints - damage_;

                cout << "Monster " << monster_id.key() << " had " << monster_hitpoints << " hitpoints, took " << damage_
                     << " damage, now has " << monster_new_hitpoints << " hitpoints" << endl;

                auto player = ctx.get(player_id);

                if (monster_new_hitpoints <= 0) {
                    // Monster is killed. The remove is just for demoing, and a more realistic examples would set a "dead" flag or similar.
                    ctx.remove(*monster);

                    const Player& player_body = player.content<Player>();

                    // the player earns experience for killing the monster
                    int experience_for_killing_monster = monster_body.experience_when_killed;
                    int player_experience = player_body.experience;
                    int player_new_experience = player_experience + experience_for_killing_monster;
                    int player_new_level = calculate_level_for_experience(player_new_experience);

                    cout << "Monster " << monster_id.key() << " was killed. Player " << player_id.key() << " gains "
                         << experience_for_killing_monster << " experience, now has level " << player_new_level << endl;

                    Player player_new_body = player_body;
                    player_new_body.experience = player_new_experience;
                    player_new_body.level = player_new_level;
                    ctx.replace(player, player_new_body);
                } else {
                    cout << "Monster " << monster_id.key() << " is damaged but alive" << endl;

                    Monster monster_new_body = monster_body;
                    monster_new_body.hitpoints = monster_new_hitpoints;
                    ctx.replace(*monster, monster_new_body);
                }
            });
        } catch (const transactions::transaction_exception& e) {
            cout << "got transaction exception {}" << e.what() << endl;
        }
    }
};

int
main(int, const char*[])
{
    const int NUM_THREADS = 4;
    couchbase::logger::set_log_levels(couchbase::logger::level::trace);
    atomic<bool> monster_exists = true;
    string bucket_name = "default";
    couchbase::cluster_credentials auth{};
    asio::io_context io;
    auto cluster = couchbase::cluster::create(io);
    if (!couchbase::logger::is_initialized()) {
        couchbase::logger::create_console_logger();
    }
    couchbase::logger::set_log_levels(couchbase::logger::level::trace);

    std::list<std::thread> io_threads;
    for (int i = 0; i < 2 * NUM_THREADS; i++) {
        io_threads.emplace_back([&io]() { io.run(); });
    }

    std::uniform_int_distribution<int> hit_distribution(1, 6);
    std::mt19937 random_number_engine; // pseudorandom number generator
    auto rand = std::bind(hit_distribution, random_number_engine);

    auto connstr = couchbase::utils::parse_connection_string("couchbase://127.0.0.1");
    auth.username = "Administrator";
    auth.password = "password";
    // first, open it.
    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster->open(couchbase::origin(auth, connstr), [barrier](std::error_code ec) { barrier->set_value(ec); });
        auto rc = f.get();
        if (rc) {
            cout << "ERROR opening cluster: " << rc.message() << endl;
            exit(-1);
        }
    }
    // now, open the `default` bucket
    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster->open_bucket(bucket_name, [barrier](std::error_code ec) { barrier->set_value(ec); });
        auto rc = f.get();
        if (rc) {
            cout << "ERROR opening bucket `" << bucket_name << "`: " << rc.message() << endl;
            exit(-1);
        }
    }

    couchbase::document_id player_id = { "default", "_default", "_default", "player_data" };
    Player player_data{ 14248, 23832, "player", 141, true, "Jane", make_uuid() };

    couchbase::document_id monster_id = { "default", "_default", "_default", "a_grue" };
    Monster monster_data{ 91, 40000, 0.19239324085462631, "monster", "Grue", make_uuid() };

    // upsert a player document
    {
        couchbase::operations::upsert_request req{ player_id };
        nlohmann::json j;
        to_json(j, player_data);
        req.value = j.dump();
        auto barrier = std::make_shared<std::promise<couchbase::operations::upsert_response>>();
        cluster->execute(req, [barrier](couchbase::operations::upsert_response resp) { barrier->set_value(resp); });
        auto f = barrier->get_future();
        auto resp = f.get();
        cout << "Upserted sample player document: " << player_id.key() << "with cas:" << resp.cas.value << endl;
    }
    // upsert a monster document
    {
        couchbase::operations::upsert_request req{ monster_id };
        nlohmann::json j;
        to_json(j, monster_data);
        req.value = j.dump();
        auto barrier = std::make_shared<std::promise<couchbase::operations::upsert_response>>();
        auto f = barrier->get_future();
        cluster->execute(req, [barrier](couchbase::operations::upsert_response resp) { barrier->set_value(resp); });
        auto resp = f.get();
        cout << "Upserted sample monster document: " << monster_id.key() << endl;
    }
    transactions::get_and_open_buckets(*cluster);
    transactions::transaction_config configuration;
    configuration.durability_level(transactions::durability_level::MAJORITY);
    configuration.cleanup_client_attempts(true);
    configuration.cleanup_lost_attempts(true);
    configuration.cleanup_window(std::chrono::seconds(5));
    transactions::transactions transactions(*cluster, configuration);
    GameServer game_server(transactions);
    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back([&rand, player_id, monster_id, &monster_exists, &game_server]() {
            while (monster_exists.load()) {
                cout << "[thread " << this_thread::get_id() << "]Monster exists -- lets hit it!" << endl;
                game_server.player_hits_monster(make_uuid(), rand() % 80, player_id, monster_id, monster_exists);
            }
        });
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    transactions.close();

    // close the cluster...
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    cluster->close([barrier]() { barrier->set_value(); });
    f.get();
    for (auto& t : io_threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}
