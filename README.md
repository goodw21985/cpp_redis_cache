# cpp_redis_cache
**windows or linux cpp client library that uses non-clustered redis services to create a distributed cache**

(note linux is not tested, although except for functional test, no windows calls should exist.)


RedisCache is a distributed cache library using Redis for the cache instances.
Each redis instance should be configured NOT as a cluster.  However it should
be configured to set a maximum size of the cache.
This library should be invoked by every process in a cluster that
wants to use this shared cache.
When a server fails, the cache will 'reshard', which will cause 1 or more shards
to have to build a cache from scratch.  When too many servers fail, some shards
will not be able to read or write key/value pairs to any server.

**SYNOPSIS:**
```c++
#include "RedisCache.hpp"
using namespace RedisCache;
std::vector<std::string> serverIPs = { "20.1.2.3", "20.1.2.43", "20.1.2.45", "20.1.2.55" };
CacheConfig config;
config.port = 8379;
config.shardCount=3;  // one spare
Cache cache(serverIPs, config);
cache.SetLoggingFile(stdout);
cache.AddServer("20.2.3.4");
cache.RemoveServer("20.1.2.3");
cache.Set("key", "value", [](reply& reply)
{
   if (reply.is_error()) printf( "failed");
});

cache.Get("key", [](reply& reply)
{
   if (reply.is_error()) printf( "failed");
   else printf(reply.as_string().c_str());
});
```