#include <string>
#include <iostream>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <couchbase/transactions.hxx>
#include <couchbase/client/cluster.hxx>

namespace uuids = boost::uuids;

using namespace std;
using namespace couchbase;

class GameServer
{
  private:
    transactions::transactions &transactions_;
    collection *collection_;

  public:
    GameServer(transactions::transactions &transactions, collection *collection) : transactions_(transactions), collection_(collection)
    {
    }

    int calculate_level_for_experience(int experience) const
    {
        return experience / 100;
    }

    void player_hits_monster(const string &action_id_, int damage_, const string &player_id, const string &monster_id)
    {
        transactions_.run([&](transactions::attempt_context &ctx) {
            transactions::transaction_document monster = ctx.get(collection_, monster_id);
            json11::Json monster_body = monster.content();

            int monster_hitpoints = monster_body["hitpoints"].int_value();
            int monster_new_hitpoints = monster_hitpoints - damage_;

            cout << "Monster " << monster_id << " had " << monster_hitpoints << " hitpoints, took " << damage_ << " damage, now has "
                 << monster_new_hitpoints << " hitpoints" << endl;

            transactions::transaction_document player = ctx.get(collection_, player_id);

            if (monster_new_hitpoints <= 0) {
                // Monster is killed. The remove is just for demoing, and a more realistic examples would set a "dead" flag or similar.
                ctx.remove(collection_, monster);

                const json11::Json &player_body = player.content();

                // the player earns experience for killing the monster
                int experience_for_killing_monster = monster_body["experienceWhenKilled"].int_value();
                int player_experience = player_body["experience"].int_value();
                int player_new_experience = player_experience + experience_for_killing_monster;
                int player_new_level = calculate_level_for_experience(player_new_experience);

                cout << "Monster " << monster_id << " was killed. Player " << player_id << " gains " << experience_for_killing_monster
                     << " experience, now has level " << player_new_level << endl;

                json11::Json::object player_new_body = player_body.object_items();
                player_new_body["experience"] = player_new_experience;
                player_new_body["level"] = player_new_level;
                ctx.replace(collection_, player, player_new_body);
            } else {
                cout << "Monster " << monster_id << " is damaged but alive" << endl;

                json11::Json::object monster_new_body = monster_body.object_items();
                monster_new_body["hitpoints"] = monster_new_hitpoints;
                ctx.replace(collection_, monster, monster_new_body);
            }
            cout << "About to commit transaction" << endl;
        });
    }
};

int main(int argc, const char *argv[])
{
    auto gen = uuids::random_generator()();
    string cluster_address = "couchbase://localhost";
    string user_name = "Administrator";
    string password = "password";
    string bucket_name = "default";

    cluster cluster(cluster_address, user_name, password);

    auto bucket = cluster.open_bucket(bucket_name);
    collection *collection = bucket->default_collection();

    transactions::configuration configuration;
    configuration.durability_level(transactions::durability_level::MAJORITY);
    transactions::transactions transactions(cluster, configuration);

    GameServer game_server(transactions, collection);

    // clang-format off
    string player_id = "player_data";
    json11::Json player_data = json11::Json::object{
        { "experience", 14248 },
        { "hitpoints", 23832 },
        { "jsonType", "player" },
        { "level", 141 },
        { "loggedIn", true },
        { "name", "Jane" },
        { "uuid", uuids::to_string(uuids::uuid{gen}) },
    };

    string monster_id = "a_grue";
    json11::Json monster_data = json11::Json::object{
        { "experienceWhenKilled", 91 },
        { "hitpoints", 4000 },
        { "itemProbability", 0.19239324085462631 },
        { "jsonType", "monster" },
        { "name", "Grue" },
        { "uuid", uuids::to_string(uuids::uuid{gen}) },
    };
    // clang-format on

    collection->upsert(player_id, player_data.dump());
    cout << "Upserted sample player document: " << player_id << endl;

    collection->upsert(monster_id, monster_data.dump());
    cout << "Upserted sample monster document: " << monster_id << endl;

    game_server.player_hits_monster(uuids::to_string(uuids::uuid{ gen }), rand() % 8000, player_id, monster_id);

    transactions.close();
    cluster.shutdown();
}
