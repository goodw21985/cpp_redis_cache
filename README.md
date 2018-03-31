# cpp_redis_cache
**Windows or linux cpp client library that uses non-clustered redis services to create a distributed cache**

(note linux is not tested, although except for functional test, no windows calls should exist.)

Each application needing a distributed cache, should link to this library.
A redis server should be started on each machine in a cluster.  The library will connect to each redis server it is told about.
The redis servers should not be configured as a cluster, and should each act as a standalone cache, which will hold a portion of the total cache.
Configure the conf file for the redis server instances to set the amount of memory reserved for caching, and other preferences you have.
This library can hold a few redis instances in reserve, in case there is a machine failure that is not fixed quickly.  If too many redis instances go down,
some parts of the cache will not respond, and so there will be less cache coverage.  When a redis instance is replaced in the pool, the cache will be empty, and
will become filled through the normal course of operations.

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

cache.Set("key", "value", [=](reply& reply)
{
   if (reply.is_error()) printf( "failed");
});

cache.Get("key", [=](reply& reply)
{
   if (reply.is_error()) printf( "failed");
   else printf(reply.as_string().c_str());
});
```

**using the HSET capabilities of redis:**

```c++
cache HashSet("key", "subkey", "value",[=](reply& reply)
{
   if (reply.is_error()) printf( "failed");
});

cache HashGet("key", "subkey", [=](reply& reply)
{
   if (reply.is_error()) printf( "failed");
   else printf(reply.as_string().c_str());
});


cache HashGetAll("key", [=](reply& reply)
{
    if (reply.is_error()) printf( "failed");
    else
    {
        auto& result = reply.as_array();
        for(auto item : result) {
  	   printf(item.as_string().c_str());
	}
    }
});
```

**Very Large Clusters**

When there is are failures in any redis instance, all of the clients need to come to a consensus on how to reconfigure the cluster.  Any client can do this, and a 
'redlock' algorithm is used to compete for a distributed lock on the cluster configuration.  Roughly, redlock requires that a client gets a lock from more than half of all 
redis instances, and if it cannot, to release all locks quickly, and then wait a random amount of time before trying to get the lock again.  Once one clients gets a lock, it will
set set the configuration into a global variable held (identically) within every redis server.  If a few redis servers lack good configuration data, it will retrieved from another.

With very large clusters (like 3000 that are used in Bing for caching - each holding 2GB of cache), lock contention gets harder.  The solution is simply to increase some values in
the CacheConfig struct, especially maxDelayBetweenLockTries.  Delays on the order of 5 or 10 minutes may be appropriate for huge clusters.  

```c++
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
 ```

**Functional Test**

The functional test is a good example of how to use the library.  It actually starts 10 instances of the redis server on your box while the functional test is running, and runs simple tests using real redis servers.
The functional test uses windows calls, so is not usable (as is) in linux.



