// RedisCache.cpp : Defines the exported functions for the DLL application.
//

#include "stdafx.h"
#include <iostream>
#include "rediscache.hpp"

namespace RedisCache
{
    map<string, map<string, string>> MockClient::allClients;
    map<string, map<string, map<string, string>>> MockClient::allHClients;
    volatile unsigned int logCritSec=0;

    FILE* CacheT<RedisPolicy>::logFile = nullptr;
    const char* CacheT<RedisPolicy>::c_shard_state_lock = "_shard_state_lock__";
    const char* CacheT<RedisPolicy>::c_shard_state = "_shard_state__";

    FILE* CacheT<MockRedisPolicy>::logFile = nullptr;
    const char* CacheT<MockRedisPolicy>::c_shard_state_lock = "\nshard_state_lock\n";
    const char* CacheT<MockRedisPolicy>::c_shard_state = "\nshard_state\n";
}
