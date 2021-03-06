// FunctionalTest.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "..\RedisCache\RedisCache.hpp"
#include <ostream>
#include <fstream>
#include <sstream>
#include <TlHelp32.h>
using namespace RedisCache;
using namespace std;


void SetupRedisDirectories(int cnt);
int StartServer(string dir);
bool SimpleTest();
bool ShardTest();
bool ReconnectTest();
bool ContentionTest();
bool HashSetTest();
void RunTest(bool(*func)(), const char*name);
void KillRunningServers();

string folders[10];
int serverProcessId[10];
int testruns = 0;
int testpasses = 0;
int main()
{
    KillRunningServers();
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
    cpp_redis::active_logger = std::unique_ptr<cpp_redis::logger>(new cpp_redis::logger);

    SetupRedisDirectories(10);
    for (int i = 0; i < 10; i++)
    {
        serverProcessId[i] = StartServer(folders[i]);
    }

    RunTest(HashSetTest, "HashSetTest");
    RunTest(SimpleTest, "SimpleTest");
    RunTest(ShardTest, "ShardTest");
    RunTest(ReconnectTest, "ReconnectTest");
    RunTest(ContentionTest, "ContentionTest");
    KillRunningServers();

    printf("tests passed %d of %d", testpasses, testruns);
    do {
        cout << '\n' << "Press a key to continue...";
    } while (cin.get() != '\n');

#ifdef _WIN32

    WSACleanup();

#endif /* _WIN32 */

    return 0;
}

void KillRunningServers()
{
    PROCESSENTRY32 entry;
    entry.dwSize = sizeof(PROCESSENTRY32);
    HANDLE snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, NULL);

    if (Process32First(snapshot, &entry) == TRUE)
    {
        while (Process32Next(snapshot, &entry) == TRUE)
        {
            if (_stricmp(entry.szExeFile, "redis-server.exe") == 0)
            {
                HANDLE hProcess = OpenProcess(PROCESS_ALL_ACCESS, FALSE, entry.th32ProcessID);

                TerminateProcess(hProcess, 1);

                CloseHandle(hProcess);
            }
        }
    }

    CloseHandle(snapshot);
}

void KillServer(int id)
{
    HANDLE explorer;
    explorer = OpenProcess(PROCESS_ALL_ACCESS, false, serverProcessId[id]);
    TerminateProcess(explorer, 1);
    WaitForSingleObject(explorer, 10000);
    CloseHandle(explorer);
}

void RestartServer(int i)
{
    serverProcessId[i] = StartServer(folders[i]);
    HANDLE explorer;
    explorer = OpenProcess(PROCESS_ALL_ACCESS, false, serverProcessId[serverProcessId[i]]);
    while (WaitForSingleObject(explorer, 0) == WAIT_OBJECT_0)
    {
        SLEEP(1);
    }

    CloseHandle(explorer);
}

void RunTest(bool(*func)(), const char*name)
{
    printf("running test %s\n", name);
    testruns++;
    if (func())
    {
        printf("test %s PASSED\n", name);
        testpasses++;
    }
    else
    {
        printf("test %s FAILED\n", name);
    }
}

int StartServer(string dir)
{
    char currentdir[1024];
    GetCurrentDirectory(1024, currentdir);
    string cmd = "RedisServerExe\\redis-server.exe ";
    cmd += currentdir;
    cmd += "\\";
    cmd += dir;
    cmd += "\\";
    cmd += "redis.windows-service.conf";

    LPTSTR szCmdline = _tcsdup(TEXT(cmd.c_str()));
    PROCESS_INFORMATION pi;
    STARTUPINFO si = { sizeof(STARTUPINFO) };

    BOOL ok = CreateProcess(NULL,
        szCmdline, nullptr, nullptr, true,
        0, nullptr, nullptr,
        &si, &pi);

    if (!ok)
    {
        auto err = GetLastError();
        printf("could not start %s", cmd.c_str());
    }

    return pi.dwProcessId;
}

void SetupRedisDirectories(int cnt)
{
    for (int i = 0; i < cnt; i++)
    {
        char buf[10];
        int port = 6379 + i;
        _itoa_s(port, buf, 10, 10);
        string foldername = buf;
        folders[i] = foldername;
        if (CreateDirectory(foldername.c_str(), NULL) ||
            ERROR_ALREADY_EXISTS == GetLastError()) {
        }
        else {
            printf("could not create folder %s\n", foldername.c_str());
            exit(0);
        }
        string filename = foldername + "\\redis.windows-service.conf";
        ofstream f(filename.c_str());
        if (!f.is_open()) {
            printf("could not create file %s\n", filename.c_str());
            exit(0);
        }
        f << "bind 127.0.0.1" << endl;
        f << "port " << port << endl;
        f << "tcp-backlog 511" << endl;
        f << "timeout 0" << endl;
        f << "tcp-keepalive 0" << endl;
        f << "databases 1" << endl;
        f << "maxmemory 1000000000" << endl;
        f << "maxmemory-policy allkeys-lru" << endl;
        f << "loglevel debug" << endl;
        f << "logfile C:\\Users\\bobgood.REDMOND\\Source\\Repos\\RedisCacheCpp\\RedisCache\\FunctionalTest\\" << port << "\\redis.log" << endl;
        f.close();
    }
}

void ClearAllLocks(Cache& cache)
{
    vector<string> keys;
    keys.push_back(CacheT<RedisPolicy>::c_shard_state_lock);
    for (auto& kvp : cache.servers)
    {
        auto&client = kvp.second.client;
        if (client.is_connected())
        {
            client.del(keys, [](CacheResponse& reply)
            {

            });
            client.commit();
        }
    }
}

bool HashSetTest()
{
    CacheConfig config;
    config.port = 6379;
    config.shardCount = 8;
    config.housekeepingInterval = 2000;
    config.failTimeout = 10000000;

    vector<string> serverIPs
        = { "127.0.0.1:6379",
        "127.0.0.1:6380",
        "127.0.0.1:6381",
        "127.0.0.1:6382",
        "127.0.0.1:6383",
        "127.0.0.1:6384",
        "127.0.0.1:6385",
        "127.0.0.1:6386",
        "127.0.0.1:6387",
        "127.0.0.1:6388" };

    Cache cache(serverIPs, config);
    if (!cache.WaitForConnection(100000))
    {
        printf("error: could not connect to a quorum of servers within the timeout\n");
        return false;
    }

    int readcnt = 0;
    volatile int writecnt = 0;
    int iters = 10;
    bool hasError = false;
    for (int i = 0; i < iters && !hasError; i++)
    {
        for (int j = 0; j < iters && !hasError; j++)
        {
            string key = "key";
            string field = "field";
            string value = "value";
            char buf[10];
            _itoa_s(i, buf, 10, 10);
            key += buf;
            value += buf;
            _itoa_s(j, buf, 10, 10);
            field += buf;
            value += buf;
            cache.HashSet(key, field, value, [key, field, value, &writecnt, &hasError](CacheResponse& reply) {
                writecnt++;
                if (reply.is_error())
                {
                    printf("error: HashSetSet(%s:%s) error %s\n", key.c_str(), field.c_str(), reply.as_string().c_str());
                    hasError = true;
                }
            });
        }
    }
    while (writecnt < iters*iters);
    for (int i = 0; i < iters && !hasError; i++)
    {
        string key = "key";
        string value = "value";
        char buf[10];
        _itoa_s(i, buf, 10, 10);
        key += buf;
        value += buf;
        cache.HashGetAll(key, [key, value, &readcnt, &hasError](CacheResponse& reply) {
            if (!reply.is_error())
            {
                auto& result = reply.as_array();
                if (result.size() != 20)
                {
                    printf("error: expected 20 results from HashGetAll\n");
                    hasError = true;
                }
                for (int i = 0; i < result.size(); i+=2)
                {
                    auto& fieldx = result[i].as_string();
                    auto& valuex = result[i+1].as_string();
                    if (valuex[5]!=key[3] || valuex[6]!=fieldx[5])
                    {
                        printf("error: unexpected value key=%s field=%s value=%s\n",key.c_str(),fieldx.c_str(),valuex.c_str());
                        hasError = true;
                    }
                }
            }
            else
            {
                printf("error: %s\n", reply.as_string().c_str());
                hasError = true;
            }

            readcnt++;
        });
    }
    while (readcnt < iters);

    ClearAllLocks(cache);


    SLEEP(100);
    if (hasError) return false;
    if (readcnt != iters || writecnt != iters*iters)
    {
        printf("error: did not get the correct number of async responses from redis\n");
        return false;
    }

    return true;
}

bool SimpleTest()
{
    CacheConfig config;
    config.port = 6379;
    config.shardCount = 8;
    config.housekeepingInterval = 2000;
    config.failTimeout = 10000000;

    vector<string> serverIPs
        = { "127.0.0.1:6379",
        "127.0.0.1:6380",
        "127.0.0.1:6381",
        "127.0.0.1:6382",
        "127.0.0.1:6383",
        "127.0.0.1:6384",
        "127.0.0.1:6385",
        "127.0.0.1:6386",
        "127.0.0.1:6387",
        "127.0.0.1:6388" };

    Cache cache(serverIPs, config);
    if (!cache.WaitForConnection(100000))
    {
        printf("error: could not connect to a quorum of servers within the timeout\n");
        return false;
    }

    int readcnt = 0;
    volatile int writecnt = 0;
    int iters = 1000;
    bool hasError = false;
    for (int i = 0; i < iters && !hasError; i++)
    {
        string key = "key";
        string value = "value";
        char buf[10];
        _itoa_s(i, buf, 10, 10);
        key += buf;
        value += buf;
        cache.Set(key, value, [key, value, &writecnt, &hasError](CacheResponse& reply) {
            writecnt++;
            if (reply.is_error())
            {
                printf("error: Set(%s) error %s\n", key.c_str(), reply.as_string().c_str());
                hasError = true;
            }
        });
        while (writecnt < i);
        cache.Get(key, [key, value, &readcnt, &hasError](CacheResponse& reply) {
            if (!reply.is_error())
            {
                string result = reply.as_string();
                if (result != value)
                {
                    printf("error: key/value read error expected %s=%s, saw %s=%s\n", key.c_str(), value.c_str(), key.c_str(), result.c_str());
                    hasError = true;
                }
            }
            else
            {
                printf("error: %s\n", reply.as_string().c_str());
                hasError = true;
            }

            readcnt++;
        });
        while (readcnt < i);

        ClearAllLocks(cache);
    }


    SLEEP(100);
    if (hasError) return false;
    if (readcnt != iters || writecnt != iters)
    {
        printf("error: did not get the correct number of async responses from redis\n");
        return false;
    }

    return true;
}

#define ASSERTT(x,msg) if (!(x)) {printf(msg); return false;}
bool ReconnectTest()
{
    CacheConfig config;
    config.port = 6379;
    config.shardCount = 8;
    config.housekeepingInterval = 2000;
    config.failTimeout = 10000;

    vector<string> serverIPs
        = { "127.0.0.1:6379",
        "127.0.0.1:6380",
        "127.0.0.1:6381",
        "127.0.0.1:6382",
        "127.0.0.1:6383",
        "127.0.0.1:6384",
        "127.0.0.1:6385",
        "127.0.0.1:6386",
        "127.0.0.1:6387",
        "127.0.0.1:6388" };

    Cache cache(serverIPs, config);
    if (!cache.WaitForConnection(10000))
    {
        printf("error: could not connect to a quorum of servers within the timeout\n");
        return false;
    }
    ASSERTT(cache.BadShardCount() == 0, "error: expected 0 bad shards\n");
    // quick shutdown and restart.  Will not reshard.
    KillServer(1);
    SLEEP(100);
    ASSERTT(cache.BadShardCount() == 0, "error: expected 0 bad shards\n");
    ASSERTT(cache.ConnectionCount() == 9, "error: expected 9 servers connected\n");
    RestartServer(1);
    for (int i = 0; i < 10000; i++)
    {
        if (cache.ConnectionCount() == 10) break;
        SLEEP(1);
    }

    if (cache.ConnectionCount() != 10)
    {
        SLEEP(10000000);
    }
    ASSERTT(cache.ConnectionCount() == 10, "error: expected 10 servers connected\n");

    ClearAllLocks(cache);

    return true;
}

bool ContentionTest()
{
    CacheConfig config;
    config.port = 6379;
    config.shardCount = 3;
    config.housekeepingInterval = 3000;
    config.failTimeout = 30000;
    config.maxDelayBetweenLockTries = 2000;

    vector<string> serverIPs
        = { "127.0.0.1:6379",
        "127.0.0.1:6380",
        "127.0.0.1:6381",
        "127.0.0.1:6382" };

    const int parallelClients = 10;
    Cache* cache[parallelClients];
    for (int i = 0; i < parallelClients; i++)
    {
        cache[i] = new Cache(serverIPs, config, 8);
        cache[i]->id = i + 1;
    }

    for (int i = 0; i < parallelClients; i++)
    {
        if (!cache[i]->WaitForConnection(100000))
        {
            printf("error: could not connect to a quorum of servers within the timeout\n");
            return false;
        }
        ASSERTT(cache[i]->BadShardCount() == 0, "error: expected 0 bad shards\n");
    }

    for (int i = 0; i < 10; i++)
    {
        KillServer(i);
        for (int j = 0; j < parallelClients; j++)
        {
            for (int k = 0; k < 100000; k++)
            {
                if (cache[j]->BadShardCount() == 0 && cache[j]->ConnectionCount() >= 3) break;
                SLEEP(1);
            }

            ASSERTT(cache[j]->BadShardCount() == 0, "error: expected 0 bad shards\n");
            ASSERTT(cache[j]->ConnectionCount() >= 3, "error: expected 3 servers connected\n");
        }

        RestartServer(i);
        for (int j = 0; j < parallelClients; j++)
        {
            if (!cache[j]->WaitForConnection(20000))
            {
                printf("error: could not connect to a quorum of servers within the timeout\n");
                for (int i = 0; i < parallelClients; i++)
                {
                    delete cache[i];
                }

                return false;
            }
        }
    }

    ClearAllLocks(*cache[0]);

    for (int i = 0; i < parallelClients; i++)
    {
        delete cache[i];
    }


    return true;
}

bool ShardTest()
{
    CacheConfig config;
    config.port = 6379;
    config.shardCount = 8;
    config.housekeepingInterval = 2000;
    config.failTimeout = 100000;

    vector<string> serverIPs;

    Cache cache(serverIPs, config);
    SLEEP(100);

    ASSERTT(cache.BadShardCount() == 8, "error: expected 8 bad shards\n");
    cache.AddServer("127.0.0.1:6379");
    SLEEP(100);
    ASSERTT(cache.BadShardCount() == 8, "error: expected 8 bad shards\n");
    cache.AddServer("127.0.0.1:6378"); // 6378: no such server
    cache.AddServer("127.0.0.1:6381");
    cache.AddServer("127.0.0.1:6382");
    cache.AddServer("127.0.0.1:6383");
    cache.AddServer("127.0.0.1:6384");
    cache.AddServer("127.0.0.1:6385");
    cache.AddServer("127.0.0.1:6386");
    cache.AddServer("127.0.0.1:6387");
    if (!cache.WaitForConnection(10000))
    {
        printf("error: could not connect to a quorum of servers within the timeout\n");
        return false;
    }
    ASSERTT(cache.BadShardCount() == 0, "error: expected 0 bad shards\n");

    cache.RemoveServer("127.0.0.1:6381");
    SLEEP(100);
    ASSERTT(cache.BadShardCount() == 1, "error: expected 8 bad shards\n");
    cache.AddServer("127.0.0.1:6388");
    if (!cache.WaitForConnection(10000))
    {
        printf("error: could not connect to a quorum of servers within the timeout\n");
        ClearAllLocks(cache);
        return false;
    }
    ASSERTT(cache.BadShardCount() == 0, "error: expected 0 bad shards\n");

    ClearAllLocks(cache);
    return true;
}

