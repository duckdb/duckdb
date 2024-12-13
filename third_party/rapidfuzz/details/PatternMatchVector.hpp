/* SPDX-License-Identifier: MIT */
/* Copyright (c) 2022 Max Bachmann */

#pragma once
#include <array>
#include <stdint.h>
#include <stdio.h>

#include <rapidfuzz/details/GrowingHashmap.hpp>
#include <rapidfuzz/details/Matrix.hpp>
#include <rapidfuzz/details/Range.hpp>
#include <rapidfuzz/details/intrinsics.hpp>

namespace duckdb_rapidfuzz::detail {

struct BitvectorHashmap {
    BitvectorHashmap() : m_map()
    {}

    template <typename CharT>
    uint64_t get(CharT key) const noexcept
    {
        return m_map[lookup(static_cast<uint64_t>(key))].value;
    }

    template <typename CharT>
    uint64_t& operator[](CharT key) noexcept
    {
        uint32_t i = lookup(static_cast<uint64_t>(key));
        m_map[i].key = static_cast<uint64_t>(key);
        return m_map[i].value;
    }

private:
    /**
     * lookup key inside the hashmap using a similar collision resolution
     * strategy to CPython and Ruby
     */
    uint32_t lookup(uint64_t key) const noexcept
    {
        uint32_t i = key % 128;

        if (!m_map[i].value || m_map[i].key == key) return i;

        uint64_t perturb = key;
        while (true) {
            i = (static_cast<uint64_t>(i) * 5 + perturb + 1) % 128;
            if (!m_map[i].value || m_map[i].key == key) return i;

            perturb >>= 5;
        }
    }

    struct MapElem {
        uint64_t key = 0;
        uint64_t value = 0;
    };
    std::array<MapElem, 128> m_map;
};

struct PatternMatchVector {
    PatternMatchVector() : m_extendedAscii()
    {}

    template <typename InputIt>
    PatternMatchVector(const Range<InputIt>& s) : m_extendedAscii()
    {
        insert(s);
    }

    size_t size() const noexcept
    {
        return 1;
    }

    template <typename InputIt>
    void insert(const Range<InputIt>& s) noexcept
    {
        uint64_t mask = 1;
        for (const auto& ch : s) {
            insert_mask(ch, mask);
            mask <<= 1;
        }
    }

    template <typename CharT>
    void insert(CharT key, int64_t pos) noexcept
    {
        insert_mask(key, UINT64_C(1) << pos);
    }

    uint64_t get(char key) const noexcept
    {
        /** treat char as value between 0 and 127 for performance reasons */
        return m_extendedAscii[static_cast<uint8_t>(key)];
    }

    template <typename CharT>
    uint64_t get(CharT key) const noexcept
    {
        if (key >= 0 && key <= 255)
            return m_extendedAscii[static_cast<uint8_t>(key)];
        else
            return m_map.get(key);
    }

    template <typename CharT>
    uint64_t get(size_t block, CharT key) const noexcept
    {
        assert(block == 0);
        (void)block;
        return get(key);
    }

    void insert_mask(char key, uint64_t mask) noexcept
    {
        /** treat char as value between 0 and 127 for performance reasons */
        m_extendedAscii[static_cast<uint8_t>(key)] |= mask;
    }

    template <typename CharT>
    void insert_mask(CharT key, uint64_t mask) noexcept
    {
        if (key >= 0 && key <= 255)
            m_extendedAscii[static_cast<uint8_t>(key)] |= mask;
        else
            m_map[key] |= mask;
    }

private:
    BitvectorHashmap m_map;
    std::array<uint64_t, 256> m_extendedAscii;
};

struct BlockPatternMatchVector {
    BlockPatternMatchVector() = delete;

    BlockPatternMatchVector(size_t str_len)
        : m_block_count(ceil_div(str_len, 64)), m_map(nullptr), m_extendedAscii(256, m_block_count, 0)
    {}

    template <typename InputIt>
    BlockPatternMatchVector(const Range<InputIt>& s) : BlockPatternMatchVector(s.size())
    {
        insert(s);
    }

    ~BlockPatternMatchVector()
    {
        delete[] m_map;
    }

    size_t size() const noexcept
    {
        return m_block_count;
    }

    template <typename CharT>
    void insert(size_t block, CharT ch, int pos) noexcept
    {
        uint64_t mask = UINT64_C(1) << pos;
        insert_mask(block, ch, mask);
    }

    /**
     * @warning undefined behavior if iterator \p first is greater than \p last
     * @tparam InputIt
     * @param first
     * @param last
     */
    template <typename InputIt>
    void insert(const Range<InputIt>& s) noexcept
    {
        uint64_t mask = 1;
        size_t i = 0;
        for (auto iter = s.begin(); iter != s.end(); ++iter, ++i) {
            size_t block = i / 64;
            insert_mask(block, *iter, mask);
            mask = rotl(mask, 1);
        }
    }

    template <typename CharT>
    void insert_mask(size_t block, CharT key, uint64_t mask) noexcept
    {
        assert(block < size());
        if (key >= 0 && key <= 255)
            m_extendedAscii[static_cast<uint8_t>(key)][block] |= mask;
        else {
            if (!m_map) m_map = new BitvectorHashmap[m_block_count];
            m_map[block][key] |= mask;
        }
    }

    void insert_mask(size_t block, char key, uint64_t mask) noexcept
    {
        insert_mask(block, static_cast<uint8_t>(key), mask);
    }

    template <typename CharT>
    uint64_t get(size_t block, CharT key) const noexcept
    {
        if (key >= 0 && key <= 255)
            return m_extendedAscii[static_cast<uint8_t>(key)][block];
        else if (m_map)
            return m_map[block].get(key);
        else
            return 0;
    }

    uint64_t get(size_t block, char ch) const noexcept
    {
        return get(block, static_cast<uint8_t>(ch));
    }

private:
    size_t m_block_count;
    BitvectorHashmap* m_map;
    BitMatrix<uint64_t> m_extendedAscii;
};

} // namespace duckdb_rapidfuzz::detail
