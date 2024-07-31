#include "assert.h"
#include "dram_wf/dramwormholefilter.hpp"

#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <openssl/rand.h>
#include <set>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <vector>

using namespace std;

#define POOL_SIZE 512 * 1024 * 1024

::std::uint64_t NowNanos()
{
    return ::std::chrono::duration_cast<::std::chrono::nanoseconds>(
               ::std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

int main()
{
    uint64_t *vals;
    uint64_t nvals = 1024 * 1024 * 8;

    vals = (uint64_t *)malloc(nvals * sizeof(vals[0]));
    RAND_bytes((unsigned char *)vals, sizeof(*vals) * nvals);
    srand(0);

    WormholeFilter wf(nvals);

    uint64_t added = 0;
    auto start_time = NowNanos();
    for (added = 0; added < nvals; added++)
    {
        if (wf.Add(vals[added]) == false)
        {
            break;
        }
    }
    cout << "Insertion throughput: " << 1000.0 * added / static_cast<double>(NowNanos() - start_time) << " MOPS" << endl;

    start_time = NowNanos();
    for (int looked = 0; looked < added; looked++)
    {
        if (wf.Contain(vals[looked]) == false)
        {
            cout << "ERROR" << endl;
        }
    }
    cout << "Lookup throughput: " << 1000.0 * added / static_cast<double>(NowNanos() - start_time) << " MOPS" << endl;

    cout << "PASS" << endl;

    return 0;
}