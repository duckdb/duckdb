/* SPDX-License-Identifier: MIT */
/* Copyright © 2022 Max Bachmann */

#pragma once
#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <type_traits>
#include <vector>

namespace duckdb_jaro_winkler {

namespace common {

/**
 * @defgroup Common Common
 * Common utilities shared among multiple functions
 * @{
 */

/* taken from https://stackoverflow.com/a/30766365/11335032 */
template <typename T>
struct is_iterator {
    static char test(...);

    template <typename U, typename = typename std::iterator_traits<U>::difference_type,
              typename = typename std::iterator_traits<U>::pointer,
              typename = typename std::iterator_traits<U>::reference,
              typename = typename std::iterator_traits<U>::value_type,
              typename = typename std::iterator_traits<U>::iterator_category>
    static long test(U&&);

    constexpr static bool value = std::is_same<decltype(test(std::declval<T>())), long>::value;
};

constexpr double result_cutoff(double result, double score_cutoff)
{
    return (result >= score_cutoff) ? result : 0;
}

template <typename T, typename U>
T ceildiv(T a, U divisor)
{
    return static_cast<T>(a / divisor) + static_cast<T>((a % divisor) != 0);
}

/**
 * Removes common prefix of two string views // todo
 */
template <typename InputIt1, typename InputIt2>
int64_t remove_common_prefix(InputIt1& first1, InputIt1 last1, InputIt2& first2, InputIt2 last2)
{
	// DuckDB passes a raw pointer, but this gives compile errors for std::
	int64_t len1 = std::distance(first1, last1);
	int64_t len2 = std::distance(first2, last2);
	const int64_t max_comparisons = std::min<int64_t>(len1, len2);
	int64_t prefix;
	for (prefix = 0; prefix < max_comparisons; prefix++) {
		if (first1[prefix] != first2[prefix]) {
			break;
		}
	}

//    int64_t prefix = static_cast<int64_t>(
//        std::distance(first1, std::mismatch(first1, last1, first2, last2).first));
    first1 += prefix;
    first2 += prefix;
    return prefix;
}

struct BitvectorHashmap {
    struct MapElem {
        uint64_t key = 0;
        uint64_t value = 0;
    };

    BitvectorHashmap() : m_map()
    {}

    template <typename CharT>
    void insert(CharT key, int64_t pos)
    {
        insert_mask(key, 1ull << pos);
    }

    template <typename CharT>
    void insert_mask(CharT key, uint64_t mask)
    {
        uint64_t i = lookup(static_cast<uint64_t>(key));
        m_map[i].key = static_cast<uint64_t>(key);
        m_map[i].value |= mask;
    }

    template <typename CharT>
    uint64_t get(CharT key) const
    {
        return m_map[lookup(static_cast<uint64_t>(key))].value;
    }

private:
    /**
     * lookup key inside the hashmap using a similar collision resolution
     * strategy to CPython and Ruby
     */
    uint64_t lookup(uint64_t key) const
    {
        uint64_t i = key % 128;

        if (!m_map[i].value || m_map[i].key == key) {
            return i;
        }

        uint64_t perturb = key;
        while (true) {
            i = ((i * 5) + perturb + 1) % 128;
            if (!m_map[i].value || m_map[i].key == key) {
                return i;
            }

            perturb >>= 5;
        }
    }

    std::array<MapElem, 128> m_map;
};

struct PatternMatchVector {
    struct MapElem {
        uint64_t key = 0;
        uint64_t value = 0;
    };

    PatternMatchVector() : m_map(), m_extendedAscii()
    {}

    template <typename InputIt1>
    PatternMatchVector(InputIt1 first, InputIt1 last) : m_map(), m_extendedAscii()
    {
        insert(first, last);
    }

    template <typename InputIt1>
    void insert(InputIt1 first, InputIt1 last)
    {
        uint64_t mask = 1;
        for (int64_t i = 0; i < std::distance(first, last); ++i) {
            auto key = first[i];
            if (key >= 0 && key <= 255) {
                m_extendedAscii[static_cast<size_t>(key)] |= mask;
            }
            else {
                m_map.insert_mask(key, mask);
            }
            mask <<= 1;
        }
    }

    template <typename CharT>
    void insert(CharT key, int64_t pos)
    {
        uint64_t mask = 1ull << pos;
        if (key >= 0 && key <= 255) {
            m_extendedAscii[key] |= mask;
        }
        else {
            m_map.insert_mask(key, mask);
        }
    }

    template <typename CharT>
    uint64_t get(CharT key) const
    {
        if (key >= 0 && key <= 255) {
            return m_extendedAscii[static_cast<size_t>(key)];
        }
        else {
            return m_map.get(key);
        }
    }

    /**
     * combat func for BlockPatternMatchVector
     */
    template <typename CharT>
    uint64_t get(int64_t block, CharT key) const
    {
        (void)block;
        assert(block == 0);
        return get(key);
    }

private:
    BitvectorHashmap m_map;
    std::array<uint64_t, 256> m_extendedAscii;
};

struct BlockPatternMatchVector {
    BlockPatternMatchVector() : m_block_count(0)
    {}

    template <typename InputIt1>
    BlockPatternMatchVector(InputIt1 first, InputIt1 last) : m_block_count(0)
    {
        insert(first, last);
    }

    template <typename CharT>
    void insert(int64_t block, CharT key, int pos)
    {
        uint64_t mask = 1ull << pos;

        assert(block < m_block_count);
        if (key >= 0 && key <= 255) {
            m_extendedAscii[static_cast<size_t>(key * m_block_count + block)] |= mask;
        }
        else {
            m_map[static_cast<size_t>(block)].insert_mask(key, mask);
        }
    }

    template <typename InputIt1>
    void insert(InputIt1 first, InputIt1 last)
    {
        int64_t len = std::distance(first, last);
        m_block_count = ceildiv(len, 64);
        m_map.resize(static_cast<size_t>(m_block_count));
        m_extendedAscii.resize(static_cast<size_t>(m_block_count * 256));

        for (int64_t i = 0; i < len; ++i) {
            int64_t block = i / 64;
            int64_t pos = i % 64;
            insert(block, first[i], static_cast<int>(pos));
        }
    }

    /**
     * combat func for PatternMatchVector
     */
    template <typename CharT>
    uint64_t get(CharT key) const
    {
        return get(0, key);
    }

    template <typename CharT>
    uint64_t get(int64_t block, CharT key) const
    {
        assert(block < m_block_count);
        if (key >= 0 && key <= 255) {
            return m_extendedAscii[static_cast<size_t>(key * m_block_count + block)];
        }
        else {
            return m_map[static_cast<size_t>(block)].get(key);
        }
    }

private:
    std::vector<BitvectorHashmap> m_map;
    std::vector<uint64_t> m_extendedAscii;
    int64_t m_block_count;
};

/**@}*/

} // namespace common
} // namespace duckdb_jaro_winkler
