#ifndef WORMHOLE_FILTER_H_
#define WORMHOLE_FILTER_H_

#include <algorithm>
#include <bitset>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <random>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>

#define BIT_PER_TAG 16
#define BIT_PER_FPT 12
#define TAG_PER_BUK 4

#define TAG_MASK (0xFFFFULL)
#define DIS_MASK (0x000FULL)
#define FPT_MASK (0xFFF0ULL)

#define MAX_PROB 16

#define MOD(idx, num_buckets_) ((idx) & (num_buckets_ - 1))

#define haszero16(x) (((x) - 0x0001000100010001ULL) & (~(x)) & 0x8000800080008000ULL)
#define hasvalue16(x, n) (haszero16((x) ^ (0x0001000100010001ULL * (n))))

using namespace std;

class DRAMWFTwoIndependentMultiplyShift
{
    unsigned __int128 multiply_, add_;

  public:
    DRAMWFTwoIndependentMultiplyShift()
    {
        ::std::random_device random;
        for (auto v : {&multiply_, &add_})
        {
            *v = random();
            for (int i = 1; i <= 4; ++i)
            {
                *v = *v << 32;
                *v |= random();
            }
        }
    }

    uint64_t operator()(uint64_t key) const
    {
        // If the input data has already been hashed, there is no need to re-hash the input data.
        return key;
        // return (add_ + multiply_ * static_cast<decltype(multiply_)>(key)) >> 64;
    }
};

class WormholeFilter
{
  public:
    static const uint32_t kBytesPerBucket = (BIT_PER_TAG * TAG_PER_BUK + 7) >> 3;
    static const uint32_t kTagMask = (1ULL << BIT_PER_TAG) - 1;

    struct Bucket
    {
        char bits_[kBytesPerBucket];
    } __attribute__((__packed__));

    Bucket *buckets_;

    uint32_t num_items_;
    uint32_t num_buckets_;

    DRAMWFTwoIndependentMultiplyShift hasher_;

    inline uint32_t IndexHash(uint32_t hv) const
    {
        return hv & (num_buckets_ - 1);
    }

    inline uint32_t TagHash(uint32_t hv) const
    {
        uint32_t tag;
        tag = hv & ((1ULL << BIT_PER_FPT) - 1);
        tag += (tag == 0);
        return tag;
    }

    inline uint64_t upperpower2(uint64_t x)
    {
        x--;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        x |= x >> 32;
        x++;
        return x;
    }

    inline uint32_t ReadTag(const uint32_t i, const uint32_t j) const
    {
        uint32_t i_m = MOD(i, num_buckets_);
        const char *p = buckets_[i_m].bits_;
        uint32_t tag;
        p += (j << 1);
        tag = *((uint16_t *)p);
        return tag & kTagMask;
    }

    inline void WriteTag(const uint32_t i, const uint32_t j, const uint32_t t)
    {
        uint32_t i_m = MOD(i, num_buckets_);
        char *p = buckets_[i_m].bits_;
        uint32_t tag = t & kTagMask;
        ((uint16_t *)p)[j] = tag;
    }

  public:
    explicit WormholeFilter(const uint32_t max_num_keys)
        : num_items_(0), hasher_()
    {
        num_buckets_ = upperpower2(std::max<uint64_t>(1, max_num_keys / TAG_PER_BUK));

        buckets_ = new Bucket[num_buckets_];
        memset(buckets_, 0, kBytesPerBucket * num_buckets_);
    }

    ~WormholeFilter()
    {
        delete[] buckets_;
    }

    bool Add(const uint64_t &item);

    bool Contain(const uint64_t &item) const;

    bool Delete(const uint64_t &item);

    void Show();

    uint32_t SizeInBytes()
    {
        return kBytesPerBucket * num_buckets_;
    }
};

bool WormholeFilter::Add(const uint64_t &item)
{
    const uint64_t hash = hasher_(item);
    uint64_t init_buck_idx = IndexHash(hash);
    uint64_t tag = TagHash(hash >> 32);

    for (uint32_t curr_buck_idx = init_buck_idx; curr_buck_idx < init_buck_idx + num_buckets_; curr_buck_idx++)
    {
        for (uint32_t curr_tag_idx = 0; curr_tag_idx < TAG_PER_BUK; curr_tag_idx++)
        {
            if (ReadTag(curr_buck_idx, curr_tag_idx) == 0)
            {
                while ((curr_buck_idx - init_buck_idx) >= MAX_PROB)
                {
                    bool has_cadi = false;
                    for (uint32_t prob = MAX_PROB - 1; prob > 0; prob--)
                    {
                        uint32_t cadi_buck_idx = curr_buck_idx - prob;
                        bool find_cadi = false;
                        for (uint32_t cadi_tag_idx = 0; cadi_tag_idx < TAG_PER_BUK; cadi_tag_idx++)
                        {
                            uint32_t cadi_tag = ReadTag(cadi_buck_idx, cadi_tag_idx);
                            if ((cadi_tag & DIS_MASK) + prob < MAX_PROB)
                            {
                                WriteTag(curr_buck_idx, curr_tag_idx, (cadi_tag & FPT_MASK) | ((cadi_tag & DIS_MASK) + prob));
                                curr_buck_idx = cadi_buck_idx;
                                curr_tag_idx = cadi_tag_idx;
                                find_cadi = true;
                                break;
                            }
                        }
                        if (find_cadi)
                        {
                            has_cadi = true;
                            break;
                        }
                    }
                    if (!has_cadi)
                    {
                        return false;
                    }
                }
                WriteTag(curr_buck_idx, curr_tag_idx, ((tag << 4) | (curr_buck_idx - init_buck_idx)));
                return true;
            }
        }
    }
    return false;
}

bool WormholeFilter::Contain(const uint64_t &item) const
{
    const uint64_t hash = hasher_(item);

    uint64_t init_buck_idx = IndexHash(hash);
    uint64_t tag = TagHash(hash >> 32);

    for (uint32_t prob = 0; prob < MAX_PROB; prob++)
    {
        uint32_t curr_buck_idx_mod = MOD(init_buck_idx + prob, num_buckets_);
        const char *p = buckets_[curr_buck_idx_mod].bits_;

        if (hasvalue16(*((uint64_t *)p), (tag << 4) | (prob)))
        {
            return true;
        }
    }
    return false;
}

bool WormholeFilter::Delete(const uint64_t &item)
{
    const uint64_t hash = hasher_(item);

    uint64_t init_buck_idx = IndexHash(hash);
    uint64_t tag = TagHash(hash >> 32);

    for (uint32_t prob = 0; prob < MAX_PROB; prob++)
    {
        uint32_t curr_buck_idx = init_buck_idx + prob;
        for (size_t curr_tag_idx = 0; curr_tag_idx < TAG_PER_BUK; curr_tag_idx++)
        {
            if (ReadTag(curr_buck_idx, curr_tag_idx) == ((tag << 4) | (prob)))
            {
                WriteTag(curr_buck_idx, curr_tag_idx, 0);
                return true;
            }
        }
    }
    return false;
}

void WormholeFilter::Show()
{
    for (uint32_t i = 0; i < num_buckets_; i++)
    {
        for (uint32_t j = 0; j < TAG_PER_BUK; j++)
        {
            cout << std::hex << bitset<sizeof(unsigned int) * 4>(ReadTag(i, j)) << " ";
        }
        cout << endl;
    }
}

#endif // WORMHOLE_FILTER_H_