#include <string>
#include <iostream>

#include <libcouchbase/transactions.hxx>
#include <libcouchbase/cluster.hxx>

#include "json11_parser.hxx"

using namespace std;
using namespace couchbase;

string gen_uid();

class PlayerHitsMonsterLogic : public transactions::Logic
{
  private:
    Collection *collection_;
    string uid_;
    int damage_;
    string player_id_;
    string monster_id_;

  public:
    PlayerHitsMonsterLogic(Collection *collection, const string &uid, int damage, const string &player_id, const string &monster_id)
        : collection_(collection), uid_(uid), damage_(damage), player_id_(player_id), monster_id_(monster_id)
    {
    }

    int calculate_level_for_experience(int experience)
    {
        return experience / 100;
    }

    void run(transactions::AttemptContext &ctx) override
    {
        transactions::TransactionDocument monster = ctx.get(collection_, monster_id_);
        json11::Json monster_body = monster.content_as< Json11Parser >();

        int monster_hitpoints = monster_body["hitpoints"].int_value();
        int monster_new_hitpoints = monster_hitpoints - damage_;

        cout << "Monster " << monster_id_ << " had " << monster_hitpoints << " hitpoints, took " << damage_ << " damage, now has "
             << monster_new_hitpoints << " hitpoints" << endl;

        transactions::TransactionDocument player = ctx.get(collection_, player_id_);

        if (monster_new_hitpoints <= 0) {
            // Monster is killed. The remove is just for demoing, and a more realistic examples would set a "dead" flag or similar.
            ctx.remove(collection_, monster);

            json11::Json player_body = player.content_as< Json11Parser >();

            // the player earns experience for killing the monster
            int experience_for_killing_monster = monster_body["experienceWhenKilled"].int_value();
            int player_experience = player_body["experience"].int_value();
            int player_new_experience = player_experience + experience_for_killing_monster;
            int player_new_level = calculate_level_for_experience(player_new_experience);

            cout << "Monster " << monster_id_ << " was killed. Player " << player_id_ << " gains " << experience_for_killing_monster
                 << " experience, now has level " << player_new_level << endl;

            json11::Json::object player_new_body = player_body.object_items();
            player_new_body["experience"] = player_new_experience;
            player_new_body["level"] = player_new_level;
            ctx.replace(collection_, player, json11::Json(player_new_body).dump());
        } else {
            cout << "Monster " << monster_id_ << "is damaged but alive" << endl;

            json11::Json::object monster_new_body = monster_body.object_items();
            monster_new_body["hitpoints"] = monster_new_hitpoints;
            ctx.replace(collection_, player, json11::Json(monster_new_body).dump());
        }
        cout << "About to commit transaction" << endl;
    }
};

class GameServer
{
  private:
    transactions::Transactions &transactions_;
    Collection *collection_;

  public:
    GameServer(transactions::Transactions &transactions, Collection *collection) : transactions_(transactions), collection_(collection)
    {
    }

    void player_hits_monster(const string &uid, int damage, const string &player_id, const string &monster_id)
    {
        PlayerHitsMonsterLogic logic(collection_, uid, damage, player_id, monster_id);
        transactions_.run(logic);
    }
};

int main(int argc, const char *argv[])
{
    string cluster_address = "couchbase://localhost:12000";
    string user_name = "Administrator";
    string password = "password";
    string bucket_name = "gamesim-sample";

    Cluster cluster(cluster_address, user_name, password);
    Bucket *bucket = cluster.bucket(bucket_name);
    Collection *collection = bucket->default_collection();

    transactions::Configuration configuration;
    transactions::Transactions transactions(cluster, configuration);

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
        { "uuid", gen_uid() },
    };

    string monster_id = "a_grue";
    json11::Json monster_data = json11::Json::object{
        { "experienceWhenKilled", 91 },
        { "hitpoints", 4000 },
        { "itemProbability", 0.19239324085462631 },
        { "jsonType", "monster" },
        { "name", "Grue" },
        { "uuid", gen_uid() },
    };
    // clang-format on

    collection->upsert(player_id, player_data.dump());
    cout << "Upserted sample player document: " << player_id << endl;

    collection->upsert(monster_id, monster_data.dump());
    cout << "Upserted sample monster document: " << monster_id << endl;

    game_server.player_hits_monster(gen_uid(), rand() % 8000, player_id, monster_id);

    transactions.close();
    cluster.shutdown();
}

#include <random>
#include <sstream>
#include <iomanip>

string gen_uid()
{
    static thread_local mt19937_64 generator{ random_device{}() };
    uniform_int_distribution< uint64_t > dist;

    uint64_t high = dist(generator);
    uint64_t low = dist(generator);

    stringstream ss;
    ss << hex << ((high >> 32) & 0xffffffff) << "-" << ((high >> 16) & 0xffff) << "-" << (high & 0xffff) << "-" << ((low >> 48) & 0xffff)
       << "-" << (low & 0xffffffffffff);
    return ss.str();
}
