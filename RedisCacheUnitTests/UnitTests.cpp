// UnitTests.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "../RedisCache/redisCache.hpp"
using namespace RedisCache;


#define ASSERT(x, msg) if (!(x)) {printf("Assert %s\n",msg); throw msg;}
class RedisCacheUnitTests
{
public:
    void WritePattern(CacheT<MockRedisPolicy>& cache, int seed)
    {
        int size = 1024 * 128;
        for (int i = 0; i < 10; i++)
        {
            std::string value(size, (char)seed);
            for (int j = 0; j < size; j++)
            {
                value[j] = (char)(j + i + seed);
            }
            std::string key = "key";
            key += std::to_string(i);
            bool done = false;
            cache.Set(key, value, [&done](CacheResponse& reply)
            {
                done = true;
                ASSERT(!reply.is_error(), "Set failed");
            });

            while (!done)
            {
                SLEEP(1);
            }
        }
    }

    void ReadPattern(CacheT<MockRedisPolicy>& cache, int seed)
    {
        int size = 1024 * 128;
        for (int i = 0; i < 10; i++)
        {
            std::string value(size, (char)seed);
            for (int j = 0; j < size; j++)
            {
                value[j] = (char)(j + i + seed);
            }
            std::string key = "key";
            key += std::to_string(i);
            bool done = false;
            cache.Get(key, [&done, &value](CacheResponse& reply)
            {
                done = true;
                ASSERT(!reply.is_error(), "Get failed");
                ASSERT(reply.as_string() == value, "Incorrect Result");
            });

            while (!done)
            {
                SLEEP(1);
            }
        }
    }

    void WriteHashPattern(CacheT<MockRedisPolicy>& cache, int seed)
    {
        int size = 1024;
        for (int i = 0; i < 10; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                std::string value(size, (char)seed);
                for (int k = 0; k < size; k++)
                {
                    value[k] = (char)(k + i + j + seed);
                }
                std::string key = "key";
                key += std::to_string(i);
                std::string field = "field";
                field += std::to_string(j);
                bool done = false;
                cache.HashSet(key, field, value, [&done](CacheResponse& reply)
                {
                    done = true;
                    ASSERT(!reply.is_error(), "HashSet failed");
                });

                while (!done)
                {
                    SLEEP(1);
                }
            }
        }
    }

    void ReadHashPattern(CacheT<MockRedisPolicy>& cache, int seed)
    {
        int size = 1024;
        for (int i = 0; i < 10; i++)
        {
            vector<string> expected;
            for (int j = 0; j < 10; j++)
            {



                std::string value(size, (char)seed);
                for (int k = 0; k < size; k++)
                {
                    value[k] = (char)(j + i + k + seed);
                }
                expected.push_back(value);
            }

            std::string key = "key";
            key += std::to_string(i);
            bool done = false;
            cache.HashGetAll(key, [&done, &expected](CacheResponse& reply)
            {
                done = true;
                ASSERT(!reply.is_error(), "Get failed");
                ASSERT(reply.is_array(), "should return array");
                auto& parts = reply.as_array();
                ASSERT(parts.size() == 20, "expected 20 results from hashgetall");
                for (int q = 0; q < 20; q += 2)
                {
                    auto& field = parts[q].as_string();
                    auto& val = parts[q+1].as_string();
                    int n = field[5] - '0';
                    ASSERT(val == expected[n], "Incorrect HashGetAll result");
                }
            });

            while (!done)
            {
                SLEEP(1);
            }
        }
    }

    void BlobStore()
    {
        MockClient::ClearServerData();
        std::vector<std::string> serverIPs = { "25.123.81.84", "25.123.81.83", "25.123.81.81", "25.123.81.79" };

        CacheConfig config;
        config.port = 8379;
        config.shardCount = 2;
        config.failTimeout = 1000000;
        config.housekeepingInterval = 10;
        config.maxPacketSize = 20000000;

        CacheT<MockRedisPolicy> cache(serverIPs, config);
        cache.unitTestSkipDistributed = true;
        cache.unitTestSkipPing = true;
        cache.unitTestSkipLock = true;


        WritePattern(cache, 3);
        ReadPattern(cache, 3);
        WriteHashPattern(cache, 3);
        ReadHashPattern(cache, 3);
    }
    void Failover()
    {
        MockClient::ClearServerData();
        std::vector<std::string> serverIPs = { "25.123.81.84", "25.123.81.83", "25.123.81.81", "25.123.81.79" };
        CacheConfig config;
        config.port = 8379;
        config.shardCount = 2;
        config.failTimeout = 20;
        config.housekeepingInterval = 5;
        config.maxPacketSize = 20000000;

        CacheT<MockRedisPolicy> cache(serverIPs, config);
        cache.unitTestSkipDistributed = true;
        cache.unitTestSkipPing = false;
        cache.unitTestSkipLock = true;

        SLEEP(200);

        Server<MockRedisPolicy>* was0 = cache.shards[0];
        Server<MockRedisPolicy>* was1 = cache.shards[1];

        cache.UnitTestDisconnectShard(0);
        SLEEP(200);
        ASSERT(cache.shards[0] != nullptr, "shard 0 is null");
        ASSERT(cache.shards[0] != was0, "shard 0 didn't change");
        ASSERT(cache.shards[1] == was1, "shard 1 changed too early");

        cache.UnitTestDisconnectShard(1);
        SLEEP(200);
        ASSERT(cache.shards[1] != nullptr, "shard 1 is null");
        ASSERT(cache.shards[1] != was1, "shard 1 didn't change");

        cache.UnitTestDisconnectShard(0);
        SLEEP(200);
        ASSERT(cache.shards[0] == nullptr, "shard 0 isn't null");

        cache.AddServer("xxx");
        SLEEP(200);
        ASSERT(cache.shards[0] != nullptr, "shard 0 is null");

        bool valid;
        std::string state = cache.GetShardState(valid);
        vector<string> stateParts;
        CacheT<MockRedisPolicy>::Split(state, stateParts, "\t");
        ASSERT(stateParts[1] == "xxx", "wrong shard 0");
        ASSERT(stateParts[2] == "25.123.81.84", "wrong shard 1");
        WritePattern(cache, 27);
        ReadPattern(cache, 27);
    }

    bool WaitForGoodShards(CacheT<MockRedisPolicy> &cache, int timeout, int allowance = 0)
    {
        SLEEP(50);
        for (int cnt = timeout; cnt > 0; cnt--)
        {
            SLEEP(1);
            int goodcnt = 0;
            for (Server<MockRedisPolicy>* server : cache.shards)
            {
                bool good = true;
                if (server == nullptr) good = false;
                else if (!server->client.isConnected)  good = false;
                if (good) goodcnt++;
            }

            if (goodcnt >= cache.shards.size() - allowance) return true;
        }

        printf("timed out %d\n", timeout);
        return false;
    }

    void BadConnections()
    {
        MockClient::ClearServerData();
        std::vector<std::string> serverIPs = { "Down25.123.81.84", "Down25.123.81.83", "Down25.123.81.81", "25.123.81.79" };
        CacheConfig config;
        config.port = 8379;
        config.shardCount = 2;
        config.failTimeout = 100000;
        config.housekeepingInterval = 10;
        config.maxPacketSize = 20000000;

        CacheT<MockRedisPolicy> cache(serverIPs, config);
        cache.unitTestSkipDistributed = true;
        cache.unitTestSkipPing = false;
        cache.unitTestSkipLock = true;

        cache.AddServer("Down33");
        cache.AddServer("Good54");
        cache.AddServer("Good55");

        WaitForGoodShards(cache, 1000);
        cache.UnitTestDisconnectShard(0);

        WaitForGoodShards(cache, 1000);
        WritePattern(cache, 5);
        ReadPattern(cache, 5);
    }

    void EdgeCases()
    {
        MockClient::ClearServerData();
        std::vector<std::string> serverIPs1 = { "Good1", "Good2", "Good3", "Good4" };
        CacheConfig config;
        config.port = 8379;
        config.shardCount = 3;
        config.failTimeout = 100;
        config.housekeepingInterval = 10;
        config.maxPacketSize = 20000000;
        config.redLockTimeout = 30000;

        CacheT<MockRedisPolicy> cache(serverIPs1, config);
        cache.unitTestSkipDistributed = true;
        cache.unitTestSkipPing = false;
        cache.unitTestSkipLock = true;

        WaitForGoodShards(cache, 200);
        WritePattern(cache, 99);
        string bigstring(config.maxPacketSize + 200, 'a');
        cache.Set("keyx", bigstring, [](CacheResponse& reply) {
            ASSERT(reply.is_error(), "incorrectly wrote overlarge value");
        });

        cache.Get("keyx", [](CacheResponse& reply) {
            ASSERT(reply.is_null(), "incorrectly read nonexistent value");
        });
    }

    /// <summary>
    /// Simulates two libraries disagreeing over the shard layout, with the first server winning, because it got the distributed lock
    /// </summary>
    void DistributedState()
    {
        MockClient::ClearServerData();
        std::vector<std::string> serverIPs1 = { "Good1", "Good2", "Good3", "Good4" };
        std::vector<std::string> serverIPs2 = { "Good4", "Good3", "Good2", "Good1" };
        CacheConfig config;
        config.port = 8379;
        config.shardCount = 3;
        config.failTimeout = 100000;
        config.housekeepingInterval = 10;
        config.maxPacketSize = 20000000;
        config.redLockTimeout = 30000;

        {
            CacheT<MockRedisPolicy> cache1(serverIPs1, config);
            cache1.unitTestSkipDistributed = false;
            cache1.unitTestSkipPing = false;
            cache1.unitTestSkipLock = true;


            WaitForGoodShards(cache1, 200);
            WritePattern(cache1, 99);
        }
        SLEEP(200);
        {
            CacheT<MockRedisPolicy> cache2(serverIPs1, config);
            cache2.unitTestSkipDistributed = false;
            cache2.unitTestSkipPing = false;
            cache2.unitTestSkipLock = true;

            WaitForGoodShards(cache2, 1000);
            bool valid;
            std::string state = cache2.GetShardState(valid);
            vector<string> stateParts;
            CacheT<MockRedisPolicy>::Split(state, stateParts, "\t");
            ASSERT(stateParts.size() == 4, "Did not load state");
            ASSERT(stateParts[1] == "Good1", "inconsistent state");
            ASSERT(stateParts[2] == "Good2", "inconsistent state");
            ASSERT(stateParts[3] == "Good3", "inconsistent state");

            ReadPattern(cache2, 99);
        }
    }

    string GetShardServer(string name)
    {
        string state = MockClient::ReadTableEntry(name, CacheT<MockRedisPolicy>::c_shard_state);
        vector<string> stateParts;
        CacheT<MockRedisPolicy>::Split(state, stateParts, "\t");
        ASSERT(stateParts.size() == 2, "incorrect number of shards");
        return stateParts[1];
    }

    void DistributedLock()
    {
        const string lockstring = "_lockstring_";

        MockClient::ClearServerData();
        std::vector<std::string> serverIPs = { "Good1", "Good2", "Good3", "Good4" };
        CacheConfig config;
        config.port = 8379;
        config.shardCount = 1;
        config.failTimeout = 100000;
        config.housekeepingInterval = 1;
        config.maxPacketSize = 20000000;
        config.redLockTimeout = 1000;
        config.redLockTimeoutMargin = 0;
        config.maxDelayBetweenLockTries = 3;

        //
        // 1. Check the distributed lock with no contention
        //

        CacheT<MockRedisPolicy> cache(serverIPs, config);
        MockRedisPolicy::ClearAllLocks(cache.c_shard_state_lock);
        cache.unitTestSkipDistributed = false;
        cache.unitTestSkipPing = false;
        cache.unitTestSkipLock = false;

        WaitForGoodShards(cache, 100);
        ASSERT(MockClient::HasTableEntry(serverIPs[0], cache.c_shard_state), "no shard state");
        string shardServer = GetShardServer(serverIPs[0]);
        string previousShardServer = shardServer;
        ASSERT(shardServer == serverIPs[0], "wrong state");

        //
        // 2. Set one lock, crash a server in rotation.  
        //    Distributed lock will still succeed, and change the server in rotation.
        //

        MockRedisPolicy::ClearAllLocks(cache.c_shard_state_lock);
        MockRedisPolicy::SetLock(serverIPs[0], cache.c_shard_state_lock, lockstring);
        cache.UnitTestDisconnectShard(0);
        WaitForGoodShards(cache, 1000);

        shardServer = GetShardServer(serverIPs[3]);

        ASSERT(shardServer != previousShardServer, "wrong state");
        previousShardServer = shardServer;
        for (auto &kvp : cache.servers)
        {
            MockRedisPolicy::Reconnect(kvp.second.client);
        }


        //
        // 3.  Set Two locks, which prevents a quorum (a majority of the 4, or at least 3)
        //     crash a server to force a reshard.  release the locks to allow the operation to complete
        //

        MockRedisPolicy::ClearAllLocks(cache.c_shard_state_lock);
        MockRedisPolicy::SetLock(serverIPs[1], cache.c_shard_state_lock, lockstring);
        MockRedisPolicy::SetLock(serverIPs[2], cache.c_shard_state_lock, lockstring);
        cache.UnitTestDisconnectShard(0);
        SLEEP(200);

        // state should not have changed given that the lock could not be created.
        shardServer = GetShardServer(serverIPs[3]);
        ASSERT(shardServer == previousShardServer, "wrong state");
        SLEEP(200);
        ASSERT(cache.shards_invalid, "shards should have been invalidated with disconnect")
            MockRedisPolicy::ClearLock(serverIPs[2], cache.c_shard_state_lock);
        SLEEP(400);

        shardServer = GetShardServer(serverIPs[3]);
        if (shardServer == previousShardServer)
        {
            SLEEP(2000);
            shardServer = GetShardServer(serverIPs[3]);
        }

        ASSERT(shardServer != previousShardServer, "wrong state");
        previousShardServer = shardServer;
        ASSERT(!cache.lock_timed_out, "lock timed out");
        for (auto &kvp : cache.servers)
        {
            MockRedisPolicy::Reconnect(kvp.second.client);
        }
        SLEEP(200);

        //
        // 4.  Set Two locks, which prevents a quorum (a majority of the 4, or at least 3)
        //     crash a server to force a reshard.  Don't release locks.  Instead act like a 
        //     different server has succeeded in the lock, forcing this one to back down.
        //

        cache.lock_timed_out = false;
        MockRedisPolicy::ClearAllLocks(cache.c_shard_state_lock);
        MockRedisPolicy::SetLock(serverIPs[1], cache.c_shard_state_lock, lockstring);
        MockRedisPolicy::SetLock(serverIPs[2], cache.c_shard_state_lock, lockstring);
        ASSERT(!MockRedisPolicy::HasLock(serverIPs[0], cache.c_shard_state_lock), "unexpected lock");
        ASSERT(MockRedisPolicy::HasLock(serverIPs[1], cache.c_shard_state_lock, lockstring), "expected lock");
        ASSERT(MockRedisPolicy::HasLock(serverIPs[2], cache.c_shard_state_lock, lockstring), "expected lock");
        ASSERT(!MockRedisPolicy::HasLock(serverIPs[3], cache.c_shard_state_lock), "unexpected lock");

        cache.UnitTestDisconnectShard(0);
        SLEEP(100);
        ASSERT(cache.trying_to_lock, "Should be trying to get a lock");
        // state should not have changed given that the lock could not be created.
        shardServer = GetShardServer(serverIPs[3]);
        ASSERT(shardServer == previousShardServer, "wrong state");

        MockRedisPolicy::ClearLock(serverIPs[2], cache.c_shard_state_lock);
        SLEEP(100);

        shardServer = GetShardServer(serverIPs[3]);
        ASSERT(shardServer == previousShardServer, "wrong state");
        SLEEP(100);

        ASSERT(cache.trying_to_lock, "Gave up on lock too soon");
        for (auto& kvp : cache.servers)
        {
            MockClient::SetTableEntry(kvp.first, cache.c_shard_state, "1\tGood4");
        }

        SLEEP(100);
        ASSERT(!cache.trying_to_lock, "Failed to give up on lock");
        ASSERT(!MockRedisPolicy::HasLock(serverIPs[0], cache.c_shard_state_lock), "unexpected lock");
        ASSERT(MockRedisPolicy::HasLock(serverIPs[1], cache.c_shard_state_lock, lockstring), "expected lock");
        ASSERT(!MockRedisPolicy::HasLock(serverIPs[2], cache.c_shard_state_lock), "unexpected lock");
        ASSERT(!MockRedisPolicy::HasLock(serverIPs[3], cache.c_shard_state_lock), "unexpected lock");
        ASSERT(!cache.lock_timed_out, "lock timed out");

        for (auto &kvp : cache.servers)
        {
            MockRedisPolicy::Reconnect(kvp.second.client);
        }

        SLEEP(100);

        //
        // 5.  Set Two locks, which prevents a quorum (a majority of the 4, or at least 3)
        //     crash a server to force a reshard.  Don't release locks. Wait for timeout
        //

        MockRedisPolicy::ClearAllLocks(cache.c_shard_state_lock);
        MockRedisPolicy::SetLock(serverIPs[1], cache.c_shard_state_lock, lockstring);
        MockRedisPolicy::SetLock(serverIPs[2], cache.c_shard_state_lock, lockstring);
        cache.UnitTestDisconnectShard(0);
        SLEEP(200);

        MockRedisPolicy::ClearLock(serverIPs[2], cache.c_shard_state_lock);
        SLEEP(200);
        for (auto &kvp : cache.servers)
        {
            MockRedisPolicy::Reconnect(kvp.second.client);
        }

        ASSERT(cache.trying_to_lock, "Gave up on lock too soon");
        ASSERT(!cache.lock_timed_out, "timed out too soon");
        SLEEP(1000);

        if (!cache.lock_timed_out)
        {
            SLEEP(4000);
        }

        ASSERT(cache.lock_timed_out, "failed to time out");
        previousShardServer = GetShardServer(serverIPs[3]);

        //
        // 6.  Set Two locks, which prevents a quorum (a majority of the 4, or at least 3)
        //     crash a server to force a reshard.  Don't release locks.  Instead act like two 
        //     different servers has succeeded in the lock, but with identical time stamps.  this
        //     should timeout.
        //

        cache.lock_timed_out = false;
        MockRedisPolicy::ClearAllLocks(cache.c_shard_state_lock);
        MockRedisPolicy::SetLock(serverIPs[1], cache.c_shard_state_lock, lockstring);
        MockRedisPolicy::SetLock(serverIPs[2], cache.c_shard_state_lock, lockstring);
        ASSERT(!MockRedisPolicy::HasLock(serverIPs[0], cache.c_shard_state_lock), "unexpected lock");
        ASSERT(MockRedisPolicy::HasLock(serverIPs[1], cache.c_shard_state_lock, lockstring), "expected lock");
        ASSERT(MockRedisPolicy::HasLock(serverIPs[2], cache.c_shard_state_lock, lockstring), "expected lock");
        ASSERT(!MockRedisPolicy::HasLock(serverIPs[3], cache.c_shard_state_lock), "unexpected lock");

        cache.UnitTestDisconnectShard(0);
        SLEEP(100);
        ASSERT(cache.trying_to_lock, "Should be trying to get a lock");
        for (auto &kvp : cache.servers)
        {
            MockRedisPolicy::Reconnect(kvp.second.client);
        }

        // state should not have changed given that the lock could not be created.
        shardServer = GetShardServer(serverIPs[3]);
        ASSERT(shardServer == previousShardServer, "wrong state");

        MockRedisPolicy::ClearLock(serverIPs[2], cache.c_shard_state_lock);
        SLEEP(100);

        shardServer = GetShardServer(serverIPs[3]);
        ASSERT(shardServer == previousShardServer, "wrong state");

        ASSERT(cache.trying_to_lock, "Gave up on lock too soon");
        int i = 0;
        for (auto& kvp : cache.servers)
        {
            string timestamp = "88\t";
            timestamp += serverIPs[i++];
            MockClient::SetTableEntry(kvp.first, cache.c_shard_state, timestamp);
        }

        SLEEP(200);
        ASSERT(cache.trying_to_lock, "Gave up on lock too soon");
        ASSERT(!cache.lock_timed_out, "timed out too soon");
        SLEEP(1200);
        ASSERT(cache.lock_timed_out, "failed to time out");
        for (auto &kvp : cache.servers)
        {
            MockRedisPolicy::Reconnect(kvp.second.client);
        }
    }

    void ConnectBackoff()
    {
        MockClient::ClearServerData();
        std::vector<std::string> serverIPs = { "DownServer" };
        CacheConfig config;
        config.port = 8379;
        config.shardCount = 1;
        config.failTimeout = 100000000;
        config.housekeepingInterval = 1;
        config.slowestReconnectInterval = 100;
        config.maxPacketSize = 20000000;
        config.redLockTimeout = 500;
        config.redLockTimeoutMargin = 0;
        config.maxDelayBetweenLockTries = 3;

        CacheT<MockRedisPolicy> cache(serverIPs, config);
        SLEEP(200);
        auto server = cache.servers[serverIPs[0]];
        int tries = server.reconnect_tries;
        int count = cache.houseKeepingCounter + 1;

        // tries should be approximately the log2 of count
        int expected = 0;
        while (count > 1)
        {
            count >>= 1;
            expected++;
        }

        ASSERT(expected == tries, "incorrect number of reconnects");
    }

    void RemoveServers()
    {
        const string lockstring = "_lockstring_";

        MockClient::ClearServerData();
        std::vector<std::string> serverIPs = { "Good1", "Good2", "Good3", "Good4" };
        CacheConfig config;
        config.port = 8379;
        config.shardCount = 1;
        config.failTimeout = 1000000;
        config.housekeepingInterval = 1;
        config.maxPacketSize = 20000000;
        config.redLockTimeout = 500000;
        config.redLockTimeoutMargin = 0;
        config.maxDelayBetweenLockTries = 3;

        CacheT<MockRedisPolicy> cache(serverIPs, config);
        MockRedisPolicy::ClearAllLocks(cache.c_shard_state_lock);
        cache.unitTestSkipDistributed = false;
        cache.unitTestSkipPing = false;
        cache.unitTestSkipLock = false;

        SLEEP(50);
        ASSERT(MockClient::HasTableEntry(serverIPs[0], cache.c_shard_state), "no shard state");
        string shardServer = GetShardServer(serverIPs[0]);
        string previousShardServer = shardServer;
        ASSERT(shardServer == serverIPs[0], "wrong state");
        MockRedisPolicy::ClearAllLocks(cache.c_shard_state_lock);
        cache.RemoveServer("Good1");
        SLEEP(200);
        shardServer = GetShardServer(serverIPs[3]);
        ASSERT(shardServer != previousShardServer, "wrong state");
        previousShardServer = shardServer;

        cache.RemoveServer("Good2");
        cache.RemoveServer("Good3");
        cache.RemoveServer("Good4");
        MockRedisPolicy::ClearAllLocks(cache.c_shard_state_lock);
        SLEEP(200);
        ASSERT(cache.trying_to_lock, "should be trying to lock, even without servers");
        cache.AddServer("New5");
        SLEEP(200);
        cache.AddServer("New6");
        cache.AddServer("New7");
        SLEEP(400);

        shardServer = GetShardServer("New7");
        ASSERT(shardServer != previousShardServer, "wrong state");
    }
};


#define TEST(N) {\
    bool ok = true; \
    total++; \
    try {u.N();} catch(...) {ok = false;failed++;} \
    printf("TEST " #N " %s\n", ok ? "PASSED" : "FAILED"); \
    }

int main()
{
    int failed = 0;
    int total = 0;
    RedisCacheUnitTests u;
    // CacheT<MockRedisPolicy>::SetLoggingFile(stdout);
    for (int i = 0; i < 1; i++)
    {
        TEST(Failover);
        TEST(BlobStore);
        TEST(BadConnections);
        TEST(EdgeCases);
        TEST(DistributedState);
        TEST(DistributedLock);
        TEST(ConnectBackoff);
        TEST(RemoveServers);
    }

    printf("Tests Passed %d of %d\n", total - failed, total);
    do {
        cout << '\n' << "Press a key to continue...";
    } while (cin.get() != '\n');
}
