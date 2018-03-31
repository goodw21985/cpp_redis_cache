#pragma once

#include "../cpp_redis/includes/cpp_redis/cpp_redis"
#define ASSERT(x, msg) if (!(x)) {printf("Assert %s\n",msg); throw msg;}


using namespace std;
namespace RedisCache
{
    template<class Policy>
    class Server;

    struct RedisPolicy
    {
        typedef cpp_redis::client client;
        static const int min_shards = 3;
        // for unit testing
        static bool Disconnect(client& /*client*/) { return false; }
        static void ClearLock(string &/*server*/, string &/*key*/) {}
    };

    class MockClient
    {

    public:
        static map<string, map<string, string>> allClients;
        static map<string, map<string, map<string,string>>> allHClients;

        bool isConnected = false;
        map<string, string> *table = nullptr;
        map<string, map<string,string>> *htable = nullptr;
        string name;
        bool isConnectable = true;

        bool is_connected()
        {
            return isConnected;
        }

        // for unit testing
        static void SetTableEntry(string host, string key, string value)
        {
            allClients[host][key] = value;
        }

        static void ClearServerData()
        {
            for (auto& x : allClients)
            {
                x.second.clear();
            }
        }

        // for unit testing
        static bool HasTableEntry(string host, string key)
        {
            return (allClients[host].find(key) != allClients[host].end());
        }

        // for unit testing
        static string ReadTableEntry(string host, string key)
        {
            ASSERT(HasTableEntry(host, key), "missingKey");
            return  allClients[host][key];
        }

        MockClient&
            ping(const cpp_redis::client::reply_callback_t& reply_callback)
        {
            if (isConnected)
            {
                cpp_redis::reply r("OK", cpp_redis::reply::string_type::simple_string);
                reply_callback(r);
            }
            else
            {
                cpp_redis::reply r("server down", cpp_redis::reply::string_type::error);
                reply_callback(r);
            }
            return *this;
        }

        MockClient& get(const string& key, const cpp_redis::client::reply_callback_t& reply_callback)
        {
            if (table->find(key) == table->end())
            {
                cpp_redis::reply r;
                reply_callback(r);
            }
            else
            {
                cpp_redis::reply r((*table)[key], cpp_redis::reply::string_type::simple_string);
                reply_callback(r);
            }
            return *this;
        }

        MockClient& set(const string& key, const string& value, const cpp_redis::client::reply_callback_t& reply_callback)
        {
            (*table)[key] = value;
            if (isConnected)
            {
                cpp_redis::reply r("OK", cpp_redis::reply::string_type::simple_string);
                reply_callback(r);
            }
            else
            {
                cpp_redis::reply r("server down", cpp_redis::reply::string_type::error);
                reply_callback(r);
            }
            return *this;
        }

        MockClient& hgetall(const string& key, const cpp_redis::client::reply_callback_t& reply_callback)
        {
            if (htable->find(key) == htable->end())
            {
                cpp_redis::reply r;
                reply_callback(r);
            }
            else
            {
                vector<cpp_redis::reply> results;
                for (auto &kvp : (*htable)[key])
                {
                    cpp_redis::reply r1(kvp.first, cpp_redis::reply::string_type::simple_string);
                    cpp_redis::reply r2(kvp.second, cpp_redis::reply::string_type::simple_string);
                    results.push_back(r1);
                    results.push_back(r2);
                }

                cpp_redis::reply r(results);
                reply_callback(r);
            }
            return *this;
        }

        MockClient& hset(const string& key, const string& field, const string& value, const cpp_redis::client::reply_callback_t& reply_callback)
        {
            (*htable)[key][field] = value;
            if (isConnected)
            {
                cpp_redis::reply r("OK", cpp_redis::reply::string_type::simple_string);
                reply_callback(r);
            }
            else
            {
                cpp_redis::reply r("server down", cpp_redis::reply::string_type::error);
                reply_callback(r);
            }
            return *this;
        }

        void disconnect(void)
        {
            isConnected = false;
        }

        void commit()
        {

        }

        void sync_commit(std::chrono::milliseconds /*chr*/)
        {

        }

        void connect(
            const string& host = "127.0.0.1",
            size_t port = 6379,
            const  cpp_redis::client::connect_callback_t& connect_callback = nullptr,
            uint32_t /*timeout_msecs*/ = 0,
            int32_t /*max_reconnects*/ = 0,
            uint32_t /*reconnect_interval_msecs*/ = 0)
        {
            ASSERT(!isConnected, "connecting already connected client");

            if (host.size() > 4 && host[0] == 'D' && host[1] == 'o')
            {
                isConnectable = false;
            }

            if (!isConnected)
            {
                if (isConnectable)
                {
                    table = &allClients[host];
                    htable = &allHClients[host];
                    name = host;
                    isConnected = true;
                    connect_callback(host, port, cpp_redis::client::connect_state::ok);
                }
                else
                {
                    connect_callback(host, port, cpp_redis::client::connect_state::failed);
                }
            }
            else
            {
                connect_callback(host, port, cpp_redis::client::connect_state::failed);
            }
        }

        MockClient&
            set_advanced(const string& key, const string& value, bool /*ex*/, int /*ex_sec*/, bool /*px*/,
                int /*px_milli*/, bool nx, bool /*xx*/, const cpp_redis::client::reply_callback_t& reply_callback)
        {
            if (nx)
            {
                if (table->find(key) != table->end())
                {
                    cpp_redis::reply r;
                    reply_callback(r);
                    return *this;
                }
                else
                {
                    set(key, value, reply_callback);
                    return *this;
                }
            }
            else
            {
                ASSERT(nx, "only nx is supported in set_advanced lock");
            }
            return *this;
        }

        MockClient&
            eval(const string& /*script*/, int numkeys, const vector<string>& keys,
                const vector<string>& args, const cpp_redis::client::reply_callback_t& reply_callback)
        {
            if (!is_connected())
            {
                cpp_redis::reply r("server down", cpp_redis::reply::string_type::error);
                reply_callback(r);
                return *this;
            }

            ASSERT(numkeys == 1 && keys.size() == numkeys && args.size() == numkeys, "Wrong number of keys or args");
            string key = keys[0];
            string arg = args[0];
            if (table->find(key) != table->end())
            {
                if ((*table)[key] == arg)
                {
                    table->erase(key);
                    cpp_redis::reply r(1);
                    reply_callback(r);
                    return *this;
                }
                else
                {
                    cpp_redis::reply r(0);
                    reply_callback(r);
                    return *this;
                }
            }

            cpp_redis::reply r("no lock to remove", cpp_redis::reply::string_type::error);
            reply_callback(r);
            return *this;
        }
    };

    struct MockRedisPolicy
    {
        typedef MockClient client;
        static const int min_shards = 1;

        static bool Disconnect(MockClient &client)
        {
            client.isConnected = false;
            client.isConnectable = false;
            return true;
        };
        static bool Reconnect(MockClient &client)
        {
            if (client.isConnectable == false)
            {
                client.isConnected = false;
                client.isConnectable = true;
            }
            return true;
        };

        static void ClearAllLocks(const string &key)
        {
            for (auto it : MockClient::allClients)
            {
                ClearLock(it.first, key);
            }
        }

        static void ClearLock(const string &server, const string &key)
        {
            MockClient::allClients[server].erase(key);
        }

        static void SetLock(const string &server, const string &key, const string& value)
        {
            MockClient::allClients[server][key] = value;
        }

        static bool HasLock(const string &server, const string &key)
        {
            return (MockClient::allClients[server].find(key) != MockClient::allClients[server].end());
        }

        static bool HasLock(const string &server, const string &key, const string &value)
        {
            if (!HasLock(server, key)) return false;
            return (MockClient::allClients[server][key] == value);
        }
    };
}