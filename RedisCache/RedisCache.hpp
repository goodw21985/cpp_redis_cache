#pragma once
#include "util.h"
#include "../cpp_redis/includes/cpp_redis/cpp_redis"
#include "redispolicy.hpp"
#include <vector>
#include <string>
using namespace std;
/*
 * RedisCache is a distributed cache library using Redis for the cache instances.
 * Each redis instance should be configured NOT as a cluster.  However it should
 * be configured to set a maximum size of the cache.
 * This library should be invoked by every process in a cluster that
 * wants to use this shared cache.
 * When a server fails, the cache will 'reshard', which will cause 1 or more shards
 * to have to build a cache from scratch.  When too many servers fail, some shards
 * will not be able to read or write key/value pairs to any server.
 *
 * SYNOPSIS:
 * using namespace RedisCache;
 * std::vector<std::string> serverIPs = { "20.1.2.3", "20.1.2.43", "20.1.2.45", "20.1.2.55" };
 * CacheConfig config;
 * config.port = 8379;
 * config.shardCount=3;  // one spare
 * Cache cache(serverIPs, config);
 * cache.SetLoggingFile(stdout);
 * cache.AddServer("20.2.3.4");
 * cache.RemoveServer("20.1.2.3");
 * cache.Set("key", "value", [](reply& reply)
 * {
 *    if (reply.is_error()) printf( "failed");
 * });
 *
 * cache.Get("key", [](reply& reply)
 * {
 *    if (reply.is_error()) printf( "failed");
 *    else printf(reply.as_string().c_str());
 * });
 */

namespace RedisCache
{
    typedef cpp_redis::reply CacheResponse;
    typedef std::function<void(CacheResponse&)> reply_callback_t;

    /// Configuration parameters for the cache
    class CacheConfig
    {
    public:

        // Port number to use for all redis connections, unless the server ip string includes a port (192.168.1.1:8888)
        int port = 6379;

        // Number of servers that will be used actively (not held in reserve).
        int shardCount = 3;

        // time until a server is considered fully failed
        int failTimeout = 60000;

        // How often each server is pinged to see if it is alive
        int housekeepingInterval = 20000;

        // Reconnection attempts will normally occur at the housekeepingInterval.  However if
        // the reconnect fails, it will exponentially backoff, but never to more than this interval
        int slowestReconnectInterval = 600000;

        // Maximum packet size allowed in cache
        size_t maxPacketSize = 10000000;

        // Amount of time redis holds lock per instance.  should be a greater than the ping interval.
        int redLockTimeout = 120000;

        // should be a fraction of redLockTimeout, but large enough to minimize
        // contention between servers.  Actual delay is a random number smaller than this.
        int maxDelayBetweenLockTries = 1000;

        // The amount of time that should be be subtracted from redlockTimeout for the purposes of
        // giving up on trying to achieve a distributed lock once an attempt is started.  This represents
        // the time it takes to communicate with all the servers, plus clock jitter between the servers.
        int redLockTimeoutMargin = 1000;
    };

    // private class to manage each redis server that this client is watching.
    // All server objects must be kept in the servers map within the client object.
    // No server can ever be deleted, for thread safety reasons, however they
    // can be marked 'isRemoved', with the same effect as deleting it.  A removed
    // server can be readded.
    template<class Policy>
    class Server
    {
    public:
        static const int noShardAssigned = -1;

        // client connection to the Redis server->  This value will be null if it was never found. 
        // This connection may have been disconnected, or may remain connected,but unresponsive.
        typename Policy::client client;

        // The shard number (if in rotation), or noShardAssigned if the server is a spare, or is unresponsive.
        int shard;

        // If true, the server will be considered deleted, even though it must
        // remain in the servers map.
        bool isRemoved = false;

        // Time stamp of last successful ping.  Used to determine if a server has been unresponsive long enough to abandon its data.
        size_t lastSuccessfulPing;

        // name of server (ip, or ip and port, if port is provided explicitly)
        string name;

        // The number of housekeeping calls to skip between connection attempts.  This value doubles every attempt up to a maximum.
        int reconnect_backoff_rate = 1;

        // the number of housekeeping calls skipped so far.
        int reconnect_backoff_sofar = 0;

        // number of connection attempts since last success (for unit testing).
        int reconnect_tries = 0;

        // Is the server currently being pinged? (to avoid overlaps)
        size_t inPing = false;

        // connects to the server, this call is a wrapper for the redis_cpp library
        // so that the hostname can be either supplied as "127.0.0.1" or "127.0.0.1:8888"
        // to override the default port to 8888.
        void connect(
            const string& host = "127.0.0.1:8888",
            size_t defaultPort = 6379,
            const  cpp_redis::client::connect_callback_t& connect_callback = nullptr);
    };

    // The CacheT class is the public API to use this library.
    // Each process that needs to use the distributed cache, should
    // instantiate a single instance of the object.
    //
    // The Policy template  is used to supply redis mocks (for unit tests).  Production
    // should always use the Cache typedef (CacheT<RedisPolicy).
    // (unit tests should use CacheT<MockRedisPolicy>)

    template<class Policy = RedisPolicy>
    class CacheT
    {
    public:
        // Redis Key used for the distributed lock
        static const char* c_shard_state_lock;
        // Redis Key used for the distributed shard state
        static const char* c_shard_state;
        // Redis LUA scripts used.
        const string c_continueLockScript = "if redis.call('get', KEYS[1]) == ARGV[1] then redis.call('del', KEYS[1]) end return redis.call('set', KEYS[1], ARGV[2], 'px', ARGV[3], 'nx')";
        const string c_unlockScript = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

        // the configuration parameters, as passed in the constructor
        const CacheConfig config;

    private:
        // A background thread that pings servers, reconnects servers that go down,
        // and uses a distributed lock to create agreement amongst all the client 
        // libraries over how the cluster is sharded.
        thread houseKeepingThread;

        // True is the object has been deleted.  Is used to shut down background activity.
        bool isDisposed = false;

        // If null, no logging is done. Else is the file used for logging.
        static FILE *logFile;

        // threadsafe protection of server map
        volatile unsigned int serverCritSec = 0;

        static const int lambdaCancelationPoolSize = 128 * 1024;
        CancelationTokenPool lambdaCancelationTokenPool;
        int lambdaCancelationTokenPoolNext = 0;

        Random rand;

    public:
        // Exposed for unit tests.  

        // The servers that are used for each of the shards currently.
        vector<Server<Policy>*> shards;

        // All the servers (including removed servers) that this cache is managing.
        map<string, Server<Policy>> servers;

        // When true, this library knows that some shard server is offline, and
        // then attempts to get a distributed lock, so that at least one client will
        // try to repair it.
        bool shards_invalid = false;

        // Entirely for unit tests
        bool unitTestSkipDistributed = false;
        bool unitTestSkipPing = false;
        bool unitTestSkipLock = false;
        bool trying_to_lock = false;
        bool lock_timed_out = false;
        int houseKeepingCounter = 0;
        int id = 0;

    public:
        CacheT(vector<string> &redisIps, CacheConfig &cacheconfig, int networkThreads = 1);

        ~CacheT();

        // Optional.  Wait until all the servers are connected.  returns true if they are.
        // Usually, this is not necessary, as the cache will slowly come up, and the cache
        // errors in the interim are acceptible.
        bool WaitForConnection(int timeout);

        // returns zero if the cache is functioning fully. 
        // If one or more shards are not active because of failing servers, this count is returned.
        // (used for functional testing).  For testing purposes.
        int BadShardCount();

        // returns the number of servers which are currently connected to this client.  This may include
        // servers which are not assigned to a shard (are in reserve).  For testing purposes.
        int ConnectionCount();

        // Gets a value from a key from the distributed cache.  returns the result through a callback
        void Get(string key, const reply_callback_t& reply_callback);

        // Sets a key/value in the distributed cache.  failure can be detected in 
        void Set(string key, string value, const reply_callback_t& reply_callback);

		// Gets a hashtable from the distributed cache.  returns the result through a callback
		void HashGetAll(string key, const reply_callback_t& reply_callback);

		// Gets a hashtable from the distributed cache.  returns the result through a callback
		void HashGet(string key, string field, const reply_callback_t& reply_callback);

		string GetShardNameFromKey(string key);

        // Sets a an entry (using field) in a hash table (identified by key) in the distributed cache.  failure can be detected in 
        void HashSet(string key, string field, string value, const reply_callback_t& reply_callback);

        // Adds a server to the cluster.  If the server already exists, nothing
        // is done.  If the server was removed, it will be reconnected.
        void AddServer(string server);

        // Removes a server from the collection.  The server remains in the map
        // of servers, but the server is marked removed.
        void RemoveServer(string server);

        // Returns true if a server already exists (and it not removed) in the collection
        bool ContainsServer(string server);

        // Thread safe method to set or replace the current log file.
        // Setting to nullptr will turn off logging.
        static void SetLoggingFile(FILE* log);

        // Print a line to the log file.
        void RLog(const char* fmt, ...);

    private:
        // Is true for any service that has not been responding to pings until
        // it passed the timeout interval.  removed servers cannot be unresponsive.
        // A server can be connected and unresponsive, and can be responsive and not
        // connected.  Servers that rapidly restart, may become disconnected, but remain 
        // responsive.
        bool IsNonResponsive(Server<Policy>* server) const;

        // Is there a valid connection to this server.
        bool IsConnected(Server<Policy>* server) const;

        // Calculateds the shard number for a given key.
        int Shard(string key)
        {
            return (int)(hash<string>()(key) % shards.size());
        }

        // Thread safe method to get a list of all non-removed servers.
        vector<string> GetServerList();

        // Thread safe method to get a server object from its name.  
        // removed servers will always exist, but are marked isRemoved.
        Server<Policy>* GetServer(string name);

        // Gets a timestamp (milliseconds since 1/1/1970)
        // This timestamp should be safe from people changing the system clock,
        // And should run at a similar rate on ever machine, and the 'jitter'
        // between the timestamps on multiple machines should be a few minutes or less.
        static size_t TimeStamp();

        // Runs in a private thread, periodically managing changes in the cluster
        void HouseKeeping();

        // Attempts to change which servers are in each shard when the cluster is found to
        // have dead servers in one or more of the shards.  Many clients may attempt to reshard
        // at nearly the same time.  A distributed lock is taken, and the winner does the work,
        // and the losers back down and use the distributed state that is written to each server.
        void Reshard();

        // Reads the distributed state from all the servers, and decides what the consensus decision is
        // (in case some servers were dead during resharding, there may not be unanimity.)
        // Returns false if the distributed state was found to be wanting.
        bool ReadDistributedShardState();

        // Applies the distributed state to the current cache.  
        // Returns false if the distributed state was found to be wanting.
        bool ApplyDistributedShardState(vector<string> shardState);

        // Creates a random string.
        string GetUniqueLockId();

        // Wrapper for HouseKeeping method.
        static void HouseKeepingWrapper(CacheT<Policy>* cache) { cache->HouseKeeping(); }

        // Uses the Redlock alogorithm to get a distributed lock from the available
        // redis servers.  All connected servers participate in the quorum, even if they are 
        // not currently being used as cache shards.
        bool GetDistributedLock(const string& lockId);

        // removes all locks from each resid server matching this id.
        void RollbackLocks(const string& lockId);

    public: // for unit tests
        // Builds the shard state string given the current state of this cache.
        string GetShardState(bool &valid);

        // Disconnects a mock server, and makes it unable to reconnect.  The
        // server disconnected is the nth server in shard list.
        void UnitTestDisconnectShard(int n);

        // implementation of string::split
        static void Split(const string& subject, vector<string>& container, const char* delim);
    };

    template<class Policy>
    CacheT<Policy>::CacheT(vector<string> &redisIps, CacheConfig &cacheConfig, int networkThreads)
        : config(cacheConfig)
        , lambdaCancelationTokenPool(lambdaCancelationPoolSize)
    {
        auto tt = std::chrono::system_clock::now();
        time_t t = std::chrono::system_clock::to_time_t(tt);
        struct tm timeinfo;
        localtime_s(&timeinfo, &t);

        std::chrono::duration<double> sec = tt -
            std::chrono::system_clock::from_time_t(mktime(&timeinfo));
        int microseconds = (int)(sec.count() * 1000000);
        auto n = (unsigned int)(t + microseconds + GetCurrentThreadId() + GetCurrentProcessId());
        rand.Seed(n);

        if (config.shardCount < Policy::min_shards)
        {
            throw "must have at least three shards";
        }

        while (shards.size() < config.shardCount) shards.push_back(nullptr);
        for (string s : redisIps)
        {
            AddServer(s);
        }

        if (networkThreads > 1)
        {
            tacopie::utils::get_default_thread_pool()->set_nb_threads(networkThreads);
        }

        houseKeepingThread = thread(HouseKeepingWrapper, this);
    }

    extern volatile unsigned int logCritSec;

    template<class Policy>
    void CacheT<Policy>::RLog(const char* format, ...)
    {
        if (logFile == nullptr) return;
        CritSecSleep critSec(logCritSec);
        if (logFile == nullptr)
        {
            return;
        }

        auto tt = std::chrono::system_clock::now();
        time_t t = std::chrono::system_clock::to_time_t(tt);
        char buff[20];
        struct tm timeinfo;
        localtime_s(&timeinfo, &t);

        strftime(buff, 20, "%m/%d/%y %H:%M:%S", &timeinfo);
        fprintf(logFile, buff);

        std::chrono::duration<double> sec = tt -
            std::chrono::system_clock::from_time_t(mktime(&timeinfo));
        int microseconds = (int)(sec.count() * 1000000);
        fprintf(logFile, ".%06d: ", microseconds);

        if (id > 0) fprintf(logFile, "[%d] ", id);
        va_list args;
        va_start(args, format);
        vfprintf(logFile, format, args);
        va_end(args);
        fprintf(logFile, "\n");
    }

    template<class Policy>
    vector<string>  CacheT<Policy>::GetServerList()
    {
        std::vector<string> results;
        CritSec critSec(serverCritSec);
        for (auto& it : servers)
        {
            auto* server = &it.second;
            if (!server->isRemoved)
                results.push_back(it.first);
        }

        return results;
    }

    template<class Policy>
    Server<Policy>*  CacheT<Policy>::GetServer(string name)
    {
        CritSec critSec(serverCritSec);
        return &servers[name];
    }

    template<class Policy>
    void CacheT<Policy>::SetLoggingFile(FILE* log)
    {
        CritSec critSec(logCritSec);
        if (logFile != nullptr)
        {
            fflush(logFile);

            if (logFile != stdout)
            {
                fclose(logFile);
            }
        }

        logFile = log;
    }

    template<class Policy>
    CacheT<Policy>::~CacheT()
    {
        isDisposed = true;
        houseKeepingThread.join();

        for (auto& kvp : servers)
        {
            kvp.second.isRemoved = true;
            if (kvp.second.client.is_connected())
            {
                kvp.second.client.disconnect();
                Sleep(100);
                try
                {
                    kvp.second.client.commit();
                }
                catch(...)
                { }
            }
        }

        Sleep(1000);
        servers.clear();
    }

    template<class Policy>
    void CacheT<Policy>::AddServer(string serverIp)
    {
        if (isDisposed) return;
        Server<Policy> *server;
        {
            CritSec critSec(serverCritSec);
            auto it = servers.find(serverIp);
            if (it != servers.end())
            {
                return;
            }

            server = &servers[serverIp];
        }

        server->isRemoved = false;
        server->shard = Server<Policy>::noShardAssigned;
        server->lastSuccessfulPing = 0;
        server->name = serverIp;
        server->connect(server->name.c_str(), config.port, [server, this](const string& host, size_t port, cpp_redis::client::connect_state status)
        {
            if (status == cpp_redis::client::connect_state::ok)
            {
                server->lastSuccessfulPing = TimeStamp();
                server->reconnect_backoff_rate = 1;
                server->reconnect_backoff_sofar = 0;
                RLog("%s:%d connected", host.c_str(), port);
            }
            else
            {
                RLog("%s:%d not connected", host.c_str(), port);
            }
        });
    }

    template<class Policy>
    void CacheT<Policy>::RemoveServer(string serverIp)
    {
        CritSec critSec(serverCritSec);
        auto it = servers.find(serverIp);
        if (it == servers.end()) return;
        Server<Policy> *server = &servers[serverIp];
        if (server->shard != Server<Policy>::noShardAssigned)
        {
            RLog("server %s removed from shard %d", serverIp.c_str(), server->shard);
            shards[server->shard] = nullptr;
            shards_invalid = true;
        }

        server->isRemoved = true;
        if (server->client.is_connected())
        {
            server->client.disconnect();
        }
    }

    template<class Policy>
    bool CacheT<Policy>::ContainsServer(string serverIp)
    {
        CritSec critSec(serverCritSec);
        if (isDisposed) return false;
        auto it = servers.find(serverIp);
        if (it == servers.end()) return false;
        return (!servers[serverIp].isRemoved);
    }

    template<class Policy>
    void CacheT<Policy>::Set(string key, string value, const reply_callback_t& reply_callback)
    {
        if (isDisposed) return;


        if (value.size() > config.maxPacketSize)
        {
            RLog("Set(%s): packet too large, ignored. %d bytes", key.c_str(), value.size());
            CacheResponse r = { "packet too large", CacheResponse::string_type::error };
            reply_callback(r);
            return;
        }

        int shard = Shard(key);
        if (shards[shard] == nullptr)
        {
            RLog("Set(%s): server down for shard %d", key.c_str(), shard);
            CacheResponse r = { "server down", CacheResponse::string_type::error };
            reply_callback(r);
            return;
        }

        shards[shard]->client.set(key, value, reply_callback);
        try
        {
            shards[shard]->client.commit();
        }
        catch (...)
        {
        }
    }

    template<class Policy>
    void CacheT<Policy>::Get(string key, const reply_callback_t& reply_callback)
    {
        if (isDisposed) return;
        int shard = Shard(key);
        if (shards[shard] == nullptr)
        {
            RLog("Get(%s) server down for shard %d", key.c_str(), shard);
            CacheResponse r = { "server down", CacheResponse::string_type::error };
            reply_callback(r);
            return;
        }

        shards[shard]->client.get(key, reply_callback);
        try
        {
            shards[shard]->client.commit();
        }
        catch (...)
        {
        }
    }

    template<class Policy>
    void CacheT<Policy>::HashSet(string key, string field, string value, const reply_callback_t& reply_callback)
    {
        if (isDisposed) return;


        if (value.size() > config.maxPacketSize)
        {
            RLog("HashSet(%s): packet too large, ignored. %d bytes", key.c_str(), value.size());
            CacheResponse r = { "packet too large", CacheResponse::string_type::error };
            reply_callback(r);
            return;
        }

        int shard = Shard(key);
        if (shards[shard] == nullptr)
        {
            RLog("HashSet(%s): server down for shard %d", key.c_str(), shard);
            CacheResponse r = { "server down", CacheResponse::string_type::error };
            reply_callback(r);
            return;
        }

        shards[shard]->client.hset(key, field, value, reply_callback);
        try
        {
            shards[shard]->client.commit();
        }
        catch (...)
        {
        }
    }

    template<class Policy>
    std::string CacheT<Policy>::GetShardNameFromKey(string key)
    {
        int shard = Shard(key);
        return (shards[shard]->name);
    }

	template<class Policy>
	void CacheT<Policy>::HashGetAll(string key, const reply_callback_t& reply_callback)
	{
		if (isDisposed) return;
		int shard = Shard(key);
		if (shards[shard] == nullptr)
		{
			RLog("HashGetAll(%s) server down for shard %d", key.c_str(), shard);
			CacheResponse r = { "server down", CacheResponse::string_type::error };
			reply_callback(r);
			return;
		}

		shards[shard]->client.hgetall(key, reply_callback);
		try
		{
			shards[shard]->client.commit();
		}
		catch (...)
		{
		}
	}

	template<class Policy>
	void CacheT<Policy>::HashGet(string key, string field, const reply_callback_t& reply_callback)
	{
		if (isDisposed) return;
		int shard = Shard(key);
		if (shards[shard] == nullptr)
		{
			RLog("HashGetAll(%s) server down for shard %d", key.c_str(), shard);
			CacheResponse r = { "server down", CacheResponse::string_type::error };
			reply_callback(r);
			return;
		}

		shards[shard]->client.hget(key, field, reply_callback);
		try
		{
			shards[shard]->client.commit();
		}
		catch (...)
		{
		}
	}


    template<class Policy>
    bool CacheT<Policy>::WaitForConnection(int timeout)
    {

        for (int i = 0; (i < timeout) && !isDisposed; i++)
        {
            bool shards_invalid = false;

            for (int i = 0; i < shards.size(); i++)
            {
                if (shards[i] == nullptr)
                {
                    shards_invalid = true;
                    break;
                }
            }

            if (!shards_invalid) return true;

            SLEEP(1);
        }

        return false;
    }

    template<class Policy>
    int CacheT<Policy>::BadShardCount()
    {
        int cnt = 0;
        for (int i = 0; i < shards.size(); i++)
        {
            if (shards[i] == nullptr)
            {
                cnt++;
            }
        }

        return cnt;
    }

    template<class Policy>
    int CacheT<Policy>::ConnectionCount()
    {
        int cnt = 0;
        for (auto& name : GetServerList())
        {
            auto *server = GetServer(name);
            if (IsConnected(server))
            {
                cnt++;
            }
        }

        return cnt;
    }

    template<class Policy>
    void CacheT<Policy>::HouseKeeping()
    {
        while (!isDisposed)
        {
            SLEEP(config.housekeepingInterval);
            houseKeepingCounter++;
            if (isDisposed) return;

            // scan servers to see if they are:
            // unresponsive (reshard the cluster)
            // unconnected (reconnect them)
            // ok (ping them)
            for (auto& name : GetServerList())
            {
                auto *server = GetServer(name);
                if (isDisposed)
                {
                }
                else if (server->isRemoved)
                {
                }
                else if (IsNonResponsive(server) && server->shard != Server<Policy>::noShardAssigned && !unitTestSkipPing)
                {
                    RLog("Server Dead %s shard %d", server->name.c_str(), server->shard);
                    shards[server->shard] = nullptr;
                    server->shard = Server<Policy>::noShardAssigned;
                    shards_invalid = true;
                }
                else if (IsConnected(server))
                {
                    if (unitTestSkipPing)
                    {
                        server->lastSuccessfulPing = TimeStamp();
                        server->reconnect_backoff_rate = 1;
                        server->reconnect_backoff_sofar = 0;
                    }
                    else if (!server->inPing) {
                        server->inPing = true;
                        server->client.ping([server, this](CacheResponse& reply) {
                            if (!reply.is_error())
                            {
                                if (IsConnected(server))
                                {
                                    server->lastSuccessfulPing = TimeStamp();
                                }

                                server->reconnect_backoff_rate = 1;
                                server->reconnect_backoff_sofar = 0;
                            }
                            else
                            {
                                if (IsConnected(server))
                                {
                                    RLog("Connected Server Ping timed out %s", server->name.c_str());
                                }
                            }
                            server->inPing = false;
                        });
                    }
                }
                else if (!server->client.is_connected() && !unitTestSkipDistributed)
                {
                    try
                    {
                        server->connect(server->name.c_str(), config.port, [server, this](const string& host, size_t port, cpp_redis::client::connect_state status)
                        {
                            if (++server->reconnect_backoff_sofar >= server->reconnect_backoff_rate)
                            {
                                if (status == cpp_redis::client::connect_state::ok)
                                {
                                    server->reconnect_tries = 0;
                                    server->lastSuccessfulPing = TimeStamp();
                                    server->reconnect_backoff_rate = 1;
                                    server->reconnect_backoff_sofar = 0;
                                    RLog("%s:%d connected", host.c_str(), port);
                                }
                                else
                                {
                                    server->reconnect_tries++;
                                    server->reconnect_backoff_sofar = 0;
                                    server->reconnect_backoff_rate = server->reconnect_backoff_rate * 2;
                                    int max = config.slowestReconnectInterval / config.housekeepingInterval;
                                    if (max <= 0) max = 1;
                                    if (server->reconnect_backoff_rate > max)server->reconnect_backoff_rate = max;
                                }
                            }
                        });
                    }
                    catch (...)
                    {
                    }
                }
            }

            // Check if any of our shards were abandoned, to set shards_invalid.
            for (int i = 0; i < shards.size(); i++)
            {
                if (!isDisposed && shards[i] == nullptr)
                {
                    shards_invalid = true;
                    break;
                }
                else if (!isDisposed && shards[i]->isRemoved)
                {
                    RLog("Server Removed %s shard %d", shards[i]->name.c_str(), shards[i]->shard);
                    shards[i] = nullptr;
                    shards_invalid = true;
                }

            }

            // If necessary, reshard the distruted cluster
            // Since many clients may compete for the distributed lock,
            // All the losers will fail to get a lock, 
            if (!isDisposed && shards_invalid)
            {
                Reshard();
            }

            // who ever got the distributed lock will have updated the servers,
            // and here we will read those servers to get the official shard state
            if (!unitTestSkipDistributed && !isDisposed)
            {
                ReadDistributedShardState();
            }

            if (shards_invalid)
            {
                // back off locks for a retry
            }
        }
    }

    template<class Policy>
    void CacheT<Policy>::Reshard()
    {
        if (isDisposed) return;

        vector<Server<Policy>*> newShards;
        while (newShards.size() < shards.size()) newShards.push_back(nullptr);

        // Get the distributed lock.  If we fail to get the lock, we will just
        // return.  In this case some other client is probably doing the work for us.
        string lockId = GetUniqueLockId();
        if (!GetDistributedLock(lockId)) return;

        // Servers that are already assigned shards should keep them to avoid
        // tossing parts of the cache.
        for (string& name : GetServerList())
        {
            Server<Policy>* server = GetServer(name);
            if (server->shard != Server<Policy>::noShardAssigned && !server->isRemoved)
            {
                newShards[server->shard] = server;
            }
        }

        // Fill missing shards from unassigned connectd objects.
        for (string& name : GetServerList())
        {
            Server<Policy>* server = GetServer(name);
            bool ic = IsConnected(server);
            bool In = IsNonResponsive(server);
            int sd = server->shard;
            bool ir = server->isRemoved;
            if (server->shard == Server<Policy>::noShardAssigned && IsConnected(server) && !IsNonResponsive(server) && !server->isRemoved)
            {
                for (int s = 0; s < shards.size(); s++)
                {
                    if (newShards[s] == nullptr)
                    {
                        newShards[s] = server;
                        server->shard = s;
                        RLog("%s set to shard %d", server->name.c_str(), server->shard);
                        break;
                    }
                }
            }
        }

        shards_invalid = false;
        for (int s = 0; s < shards.size(); s++)
        {
            if (newShards[s] == nullptr) shards_invalid = true;
            shards[s] = newShards[s];
        }

        // This should be rare, as we check that we are likely to have enough shards 
        // before we get here.  We don't want to unnecessarily write a bad shard state
        // to the cluster.  
        if (shards_invalid)
        {
            RLog("Reshard abandoned because servers are no longer valid");
            RollbackLocks(lockId);
            return;
        }

        if (!unitTestSkipDistributed)
        {
            bool valid;
            string shardState = GetShardState(valid);
            if (!valid)
            {
                RLog("reshard abandoned because shard state is bad");
                shards_invalid = true;
            }

            for (string& name : GetServerList())
            {
                Server<Policy>* server = GetServer(name);
                if (IsConnected(server))
                {
                    server->client.set(c_shard_state, shardState, [server, shardState, this](CacheResponse& /*reply*/) {
                        RLog("wrote state data to %s : %s", server->name.c_str(), shardState.c_str());
                    });
                    try
                    {
                        server->client.commit();
                    }
                    catch (...)
                    {

                    }
                }
            }
        }

        RollbackLocks(lockId);
    }

    template<class Policy>
    string CacheT<Policy>::GetShardState(bool &valid)
    {
        valid = true;
        string result = to_string(TimeStamp());
        for (int s = 0; s < shards.size(); s++)
        {
            result += "\t";
            if (shards[s] != nullptr)
            {
                if (!IsConnected(shards[s])) valid = false;
                result += shards[s]->name;
            }
            else
            {
                valid = false;
            }
        }

        return result;
    }

    template<class Policy>
    bool CacheT<Policy>::ReadDistributedShardState()
    {
        vector<string> serverlist = GetServerList();
        volatile unsigned int cnt = (int)serverlist.size();
        vector<vector<string>> results(serverlist.size());
        int resultI = 0;
        CancelationGroup cancelations(lambdaCancelationTokenPool);
        for (string &name : serverlist)
        {
            if (isDisposed) return false;
            Server<Policy> *server = GetServer(name);
            if (IsConnected(server))
            {
                CancelationToken& lambdaCancelation = cancelations.Get();
                vector<string>* result = &results[resultI++];
                server->client.get(c_shard_state, [&cnt, result, this, &lambdaCancelation](CacheResponse& reply)
                {
                    CANCELABLESECTION(lambdaCancelation);
                    if (!isDisposed && !reply.is_error() && !reply.is_null() && reply.is_string())
                    {
                        vector<string> parts;
                        Split(reply.as_string(), parts, "\t");

                        *result = parts;
                    }
                    InterlockedDecrement(&cnt);
                });
                server->client.commit();
            }
            else
            {
                InterlockedDecrement(&cnt);
            }
        }


        int timeout = 1000;
        while (cnt > 1 && timeout-- > 0)
        {
            if (isDisposed)
            {
                cancelations.Cancel();
                SLEEP(10);
                return false;
            }
            SLEEP(1);
        }

        cancelations.Cancel();

        if (timeout <= 0)
        {
            return shards_invalid;
        }

        // The use of timestamp creates a risk of clock jitter between two servers.  Server clocks can disagree by minutes.
        // However if a result is good, all that matters is that there is a consensus. 
        int besti = -1;
        size_t bestTimestamp = 0;
        for (int i = 0; i < serverlist.size(); i++)
        {
            vector<string>& result = results[i];
            if (result.size() != 0)
            {
                size_t timestamp = stoull(result[0]);
                if (timestamp > bestTimestamp)
                {
                    bestTimestamp = timestamp;
                    besti = i;
                }
            }
        }

        //  If there are two time stamps that
        // are identical, but the results disagree, then we must trigger a new attempt to resolve shard state.
        for (int i = 0; i < serverlist.size(); i++)
        {
            vector<string>& result = results[i];
            if (result.size() != 0)
            {
                size_t timestamp = stoull(result[0]);
                if (timestamp == bestTimestamp)
                {
                    if (result.size() != results[besti].size())
                    {
                        shards_invalid = true;
                        RLog("inconsistent distributed shard state.  Force resharding");
                        return shards_invalid;
                    }
                    else {
                        for (int j = 0; j < result.size(); j++)
                        {
                            if (result[j] != results[besti][j])
                            {
                                RLog("inconsistent distributed shard state.  Force resharding");
                                shards_invalid = true;
                                return shards_invalid;
                            }
                        }
                    }
                }
            }
        }

        if (besti >= 0)
        {
            auto& bestResult = results[besti];
            if (bestResult.size() == shards.size() + 1)
            {
                return ApplyDistributedShardState(bestResult);
            }
        }

        return shards_invalid;
    }

    template<class Policy>
    bool CacheT<Policy>::ApplyDistributedShardState(vector<string> shardState)
    {
        auto serverlist = GetServerList();
        vector<int> shard_numbers(serverlist.size());

        for (int i = 0; i < shard_numbers.size(); i++)
        {
            shard_numbers[i] = Server<Policy>::noShardAssigned;
        }

        for (int s = 0; s < shards.size(); s++)
        {
            string& shardname = shardState[s + 1];
            bool found = false;
            int i = 0;
            for (string& name : GetServerList())
            {
                Server<Policy>* server = GetServer(name);
                if (server->name == shardname)
                {
                    if (server->isRemoved || IsNonResponsive(server))
                    {
                        RLog("cannot use distributed shard state because a server is not valid %s", name.c_str());
                        shards_invalid = true;
                        return shards_invalid;
                    }
                    else
                    {
                        shard_numbers[i] = s;
                        found = true;
                    }

                    break;
                }

                i++;
            }

            if (!found)
            {
                RLog("shard %d is abandoned", s);
                shards_invalid = true;
                return shards_invalid;
            }
        }

        vector<Server<Policy>*> prevShards(shards.size());
        vector<Server<Policy>*> newShards(shards.size());
        for (int s = 0; s < shards.size(); s++)
        {
            prevShards[s] = shards[s];
            newShards[s] = nullptr;
        }

        for (int i = 0; i < shard_numbers.size(); i++)
        {
            Server<Policy>* server = GetServer(serverlist[i]);
            int s = shard_numbers[i];

            if (s != Server<Policy>::noShardAssigned)
            {
                newShards[s] = server;
            }
        }

        for (int i = 0; i < newShards.size(); i++)
        {
            if (newShards[i] == nullptr)
            {
                RLog("abandoned distributed shard reading: missing shard %d", i);
                shards_invalid = true;
                return shards_invalid;
            }
        }

        shards_invalid = false;

        for (int i = 0; i < shard_numbers.size(); i++)
        {
            Server<Policy>* server = GetServer(serverlist[i]);
            server->shard = shard_numbers[i];
        }

        for (int s = 0; s < newShards.size(); s++)
        {
            shards[s] = newShards[s];
            if (newShards[s] != prevShards[s])
            {
                if (prevShards[s] == nullptr)
                {
                    RLog("%s set to shard %d (was <null>)", shards[s]->name.c_str(), s);
                }
                else
                {
                    RLog("%s set to shard %d (was %s)", shards[s]->name.c_str(), s, prevShards[s]->name.c_str());
                }
            }
        }

        return shards_invalid;
    }

    template<class Policy>
    bool CacheT<Policy>::IsNonResponsive(Server<Policy>* server) const
    {
        auto ts = TimeStamp();
        if (server->isRemoved) return false;
        bool nonresponsive = (server->lastSuccessfulPing == 0 || TimeStamp() - server->lastSuccessfulPing > config.failTimeout);
        return nonresponsive;
    }

    template<class Policy>
    bool CacheT<Policy>::IsConnected(Server<Policy>* server) const
    {
        if (server == nullptr) return false;
        if (server->isRemoved) return false;
        return server->client.is_connected();
    }

    template<class Policy>
    size_t CacheT<Policy>::TimeStamp()
    {
        return chrono::duration_cast<chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
    }


    template<class Policy>
    void CacheT<Policy>::UnitTestDisconnectShard(int n)
    {
        // for unit testing only.
        for (int i = 0; i < 10; i++)
        {
            for (string& name : GetServerList())
            {
                Server<Policy>* server = GetServer(name);
                if (server->shard == n)
                {
                    if (Policy::Disconnect(server->client))
                    {
                        server->lastSuccessfulPing = 0;
                    }

                    RLog("Request to disconnect %s from shard %d (connected?=%d)", name.c_str(), n, server->client.is_connected());
                    return;
                }
            }

            SLEEP(5);
        }
        RLog("there was no server to disconnect from shard %d", n);
    }

    template<class Policy>
    void CacheT<Policy>::Split(const string& subject, vector<string>& container, const char* delim)
    {
        char *next_token1 = NULL;

        container.clear();
        size_t len = subject.length() + 1;
        char* s = new char[len];
        memset(s, 0, len * sizeof(char));
        memcpy(s, subject.c_str(), (len - 1) * sizeof(char));
        for (char *p = strtok_s(s, delim, &next_token1); p != NULL; p = strtok_s(NULL, delim, &next_token1))
        {
            container.push_back(p);
        }
        delete[] s;
    }

    template<class Policy>
    string CacheT<Policy>::GetUniqueLockId()
    {
        char buf[128];
        char* bp = buf;
        for (int i = 0; i < 10; i++)
        {
            auto r = rand.Next();
            bp += sprintf_s(bp, 128 - (bp - buf), "%03d", r % 1000);
        }
        bp += 0;

        string result = buf;
        return result;
    }

    // GetDistributedLock()
    //
    // Based on RedLock published algorithm and opensource code.
    // 
    // Attempts to get a lock from each server (non removed).
    // If we get more than half of the servers locked, we own the distributed lock.
    // If we get fewer, we remove our locks, and try again.
    // If we succeed we DO NOT remove the locks, and let them expire automatically.
    // Waiting for the locks to expire, guarantees that resharding wont churn too quickly.
    //
    // A client will also check to see if another client got a distributed lock
    // that changed the shard state in the servers.  If this occurs, and a valid shard 
    // state is discovered, this client will abandon the attempt to get a lock.
    template<class Policy>
    bool CacheT<Policy>::GetDistributedLock(const string& lockId)
    {
        if (unitTestSkipLock) return true;
        trying_to_lock = true;
        size_t start = TimeStamp();
        // sample the state placed on each server->  We will monitor if they have changed, which means someone
        // else won the lock.

        // implements redlock  must be called from ping thread, so we can use sleep
        for (;;)
        {
            if (isDisposed) return false;
            if (!ReadDistributedShardState())
            {
                RLog("abandoned lock acquisition because someone else already took care of it.");
                trying_to_lock = false;
                return false;
            }

            volatile unsigned int tries = 0;
            size_t total = 0;
            unsigned int locks = 0;
            auto activeServers = GetServerList();
            CancelationGroup cancelations(lambdaCancelationTokenPool);
            for (string& name : activeServers)
            {
                Server<Policy>* server = GetServer(name);
                if (IsConnected(server))
                {
                    total++;
                    CancelationToken& lambdaCancelation = cancelations.Get();
                    server->client.set_advanced(c_shard_state_lock, lockId, false, 0, true, config.redLockTimeout, true, false,
                        [&locks, &tries, lockId, name, &lambdaCancelation, this](CacheResponse& reply)
                    {
                        CANCELABLESECTION(lambdaCancelation);
                        if (reply.is_error())
                        {
                            //                  RLog(error writing lock %s", name.c_str());
                        }
                        else if (reply.is_null())
                        {
                            //       RLog("Lock already taken %s", name.c_str());
                        }
                        else
                        {
                            RLog("wrote lock to %s: %s", name.c_str(), lockId.c_str());
                            locks++;
                        }
                        InterlockedIncrement(&tries);
                    });
                    try
                    {
                        server->client.commit();
                    }
                    catch (...)
                    {

                    }
                }
                else if (!server->isRemoved)
                {
                    InterlockedIncrement(&tries);
                }
            }

            int timeout = 1000;
            while (tries < activeServers.size() && !isDisposed && (timeout-- > 0) && !isDisposed)
            {
                SLEEP(1);
            }

            cancelations.Cancel();
            if (isDisposed) return false;

            if ((locks > total / 2) && locks >= 3)
            {
                RLog("got distributed lock: %d of %d", locks, total);
                trying_to_lock = false;
                return true;
            }

            RLog("insufficient distributed lock: %d of %d", locks, total);
            // undo any locks made
            tries = 0;
            total = 0;

            RollbackLocks(lockId);
            // We got the lock too late, consider this a failure.
            if (TimeStamp() - start > config.redLockTimeout - config.redLockTimeoutMargin)
            {
                trying_to_lock = false;
                lock_timed_out = true;
                return false;
            }

            // Lower the priority of anyone who failed to get any locks, in the theory that there
            // are enough who did get some locks, that they should be allowed to work with less contention.
            if (!isDisposed)
            {
                unsigned delay = config.maxDelayBetweenLockTries;
                if (locks != 0)
                {
                    delay = rand.Next() % config.maxDelayBetweenLockTries;
                }

                RLog("delay %d", delay);
                for (unsigned int i = 0; i < delay; i += config.housekeepingInterval)
                {
                    if (i>=1 && !ReadDistributedShardState())
                    {
                        RLog("abandoned lock acquisition because someone else already took care of it.");
                        trying_to_lock = false;
                        return false;
                    }

                    SLEEP(min(config.housekeepingInterval,(int)delay));
                }
            }
        }
    }

    template<class Policy>
    void CacheT<Policy>::RollbackLocks(const string &lockId)
    {
        if (unitTestSkipLock) return;
        vector<string> keys;
        keys.push_back(c_shard_state_lock);
        vector<string> ids;
        ids.push_back(lockId);

        auto serverList = GetServerList();
        size_t total = 0;
        volatile unsigned tries = 0;
        CancelationGroup cancelations(lambdaCancelationTokenPool);
        for (string& name : serverList)
        {
            Server<Policy>* server = GetServer(name);
            if (IsConnected(server))
            {
                total++;
                CancelationToken& lambdaCancelation = cancelations.Get();
                server->client.eval(c_unlockScript, 1, keys, ids,
                    [server, lockId, this, &lambdaCancelation, &tries](CacheResponse& reply)
                {
                    CANCELABLESECTION(lambdaCancelation);
                    if (reply.is_integer())
                    {
                        int64_t val = reply.as_integer();
                        if (val != 0)
                        {
                            RLog("lock removed %s", server->name.c_str());
                        }
                    }
                    else if (reply.is_error())
                    {
                        server->client.get(c_shard_state_lock,
                            [server, lockId, reply, this](CacheResponse& reply2)
                        {
                            if (!reply2.is_null())
                            {
                                RLog("lock removal error %s: %s, orig lock=%s removed lock=%s", server->name.c_str(), reply.as_string().c_str(), reply2.as_string().c_str(), lockId.c_str());
                            }
                        });
                        try
                        {
                            server->client.commit();
                        }
                        catch (...)
                        {

                        }

                        InterlockedIncrement(&tries);
                    }
                    else
                    {
                        throw "unexpected result from redis eval";
                    }

                    InterlockedIncrement(&tries);
                });
                try
                {
                    server->client.commit();
                }
                catch (...)
                {

                }
            }
            else
            {
                InterlockedIncrement(&tries);
            }
        }

        int timeout = 1000;
        while ((tries < total) && !isDisposed && (timeout-- > 0))
        {
            SLEEP(1);
        }

        cancelations.Cancel();
    }

    template<class Policy>
    void Server<Policy>::connect(
        const string& host,
        size_t defaultPort,
        const  cpp_redis::client::connect_callback_t& connect_callback)
    {
        std::size_t found = host.find(":");
        if (found != std::string::npos)
        {
            vector<string> parts;
            CacheT<Policy>::Split(host, parts, ":");
            if (parts.size() != 2) throw "badly formatted url";
            int port = atoi(parts[1].c_str());
            if (port == 0)  throw "badly formatted url";
            try
            {
                client.connect(parts[0], port, connect_callback);
            }
            catch (...)
            {

            }
        }
        else
        {
            try
            {
                client.connect(host, defaultPort, connect_callback);
            }
            catch (...)
            {

            }
        }
    }

    typedef CacheT<RedisPolicy> Cache;
}