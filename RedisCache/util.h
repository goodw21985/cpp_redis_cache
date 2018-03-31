#pragma once
#include "stdafx.h"
#include <windows.h>
#include <vector>
#include <time.h> 
#include <chrono>
#include <thread>

using namespace std;
namespace RedisCache
{
    /* 
     * Cancelation Tokens are similar to critical sections that are used inside
     * lambda functions where we need to support cancelation.  If the lambda function
     * is called after the function scope has been abandoned, the system will crash, so there
     * needs to be a reliable way to notify the lambda that its inputs are no longer valid,
     * and its outputs will be ignored.
     *


     * Synopsis of Cancelation Token classes:
     * 
     * CancelationTokenPool pool();
     * auto& token = pool.Get();
     * methodWithLambda(..., [&token](reply& reply)
     * {
     *       CANCELABLE(token);
     *       (do lambda work)
     * });
     *
     * (Wait for results or timeout)
     * token.Cancel();


     * Synopsis of multiple lambdas called in method
     *
     * CancelationTokenPool pool();
     * CancelationGroup group(pool);
     * for(...)
     * {
     *       auto& token = group.Get();
     *       methodWithLambda(..., [&token](reply& reply)
     * {
     *       CANCELABLE(token);
     *       (do lambda work)
     * });
     * 
     * (Wait for results or timeout)
     * group.Cancel();
     *


     * Synopsis of CritSec
     * 
     * volatile long lock=0;
     * {
     *    CritSec anyname(lock);
     *    (do work that can only allow one thread at a time here)
     * }
     *


     * Synopsis of Random
     *
     * Random rand;
     * rand.Seed(123455);
     * printf("%d",rand.Next());
     *
     */

#define SLEEP(x) std::this_thread::sleep_for(std::chrono::milliseconds(x));

     // Spin lock critical section for performance 
    class CritSec
    {
        volatile unsigned int *lock;
    public:
        CritSec(volatile unsigned int &lockref)
            : lock(&lockref)
        {
            while (1 == InterlockedCompareExchange(lock, 1, 0))
            {

            }
        }

        ~CritSec()
        {
            *lock = 0;
        }
    };

    // Spin lock critical section for performance 
    class CritSecSleep
    {
        volatile unsigned int *lock;
    public:
        CritSecSleep(volatile unsigned int &lockref)
            : lock(&lockref)
        {
            int cnt = 0;
            while (1 == InterlockedCompareExchange(lock, 1, 0))
            {
                if (cnt++ > 10000)
                {
                    SLEEP(1);
                    cnt = 0;
                }
            }
        }

        ~CritSecSleep()
        {
            *lock = 0;
        }
    };

    // thread safe state machine state value for a lamdba function
    // (reset) - lambda function has not run yet, but we would like it to.  Or it has completed.
    // (start) lambda function is running.
    // (cancel) lambda function should not run, unless it has already started.
    // The state progression is either:  reset->start->reset->cancel  (completed action)
    // or                                reset->cancel (will never run).
    class CancelationToken
    {
    private:
        static const short reset = 0;
        static const short start = 1;
        static const short cancel = 2;
        volatile  short token;

    public:
        CancelationToken()
        {
            Reset();
        }

        void Reset()
        {
            token = reset;
        }

        void Cancel()
        {
            while (InterlockedCompareExchange16(&token, cancel, reset) != reset);
        }

        void Start()
        {
            while (InterlockedCompareExchange16(&token, start, reset) != reset) if (token == cancel) break;
        }

        bool IsCanceled()
        {
            return token == cancel;
        }
    };

    class CancelationTokenPool
    {
        std::vector<CancelationToken> pool;
        int size;
        int next;
        volatile unsigned int lambdaCancelationPoolCritSec = 0;
    public:
        CancelationTokenPool(int size0)
            : pool(size0)
        {
            size = size0;
            next = 0;
        }

        CancelationToken& Get()
        {
            CritSec critSec(lambdaCancelationPoolCritSec);
            int i = next++;
            if (next >= size) next = 0;
            return pool[i];
        }
    };

#define CANCELABLESECTION(x)  CancelableCriticalSection _ccs(x); \
                              if (x.IsCanceled()) return; 

    class CancelableCriticalSection
    {
        CancelationToken& token;
    public:
        CancelableCriticalSection(CancelationToken& t)
            : token(t)
        {
            t.Start();
        }

        ~CancelableCriticalSection()
        {
            token.Reset();
        }
    };

    class CancelationGroup
    {
        vector<CancelationToken*> group;
        CancelationTokenPool& pool;
    public:
        CancelationGroup(CancelationTokenPool& p)
            : pool(p)
        {}

        CancelationToken& Get()
        {
            auto& token = pool.Get();
            group.push_back(&token);
            return token;
        }

        void Cancel()
        {
            for (auto* token : group)
            {
                token->Cancel();
            }
        }
    };

    class Random
    {
        unsigned int next;
    public:
        Random(unsigned int seed = 12345)
        {
            next = seed;
        }

        void Seed(unsigned int seed)
        {
            next = seed;
        }

        unsigned int Next()
        {
            next = (next * 49537 + 26557) ;
            return next & RAND_MAX;
        }
    };
}
