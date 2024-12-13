/* SPDX-License-Identifier: MIT */
/* Copyright (c) 2022 Max Bachmann */

#pragma once

#include <array>
#include <stddef.h>
#include <stdint.h>

namespace duckdb_rapidfuzz::detail {

/* hashmap for integers which can only grow, but can't remove elements */
template <typename T_Key, typename T_Entry>
struct GrowingHashmap {
    using key_type = T_Key;
    using value_type = T_Entry;
    using size_type = unsigned int;

private:
    static constexpr size_type min_size = 8;
    struct MapElem {
        key_type key;
        value_type value = value_type();
    };

    int used;
    int fill;
    int mask;
    MapElem* m_map;

public:
    GrowingHashmap() : used(0), fill(0), mask(-1), m_map(nullptr)
    {}
    ~GrowingHashmap()
    {
        delete[] m_map;
    }

    GrowingHashmap(const GrowingHashmap& other) : used(other.used), fill(other.fill), mask(other.mask)
    {
        int size = mask + 1;
        m_map = new MapElem[size];
        std::copy(other.m_map, other.m_map + size, m_map);
    }

    GrowingHashmap(GrowingHashmap&& other) noexcept : GrowingHashmap()
    {
        swap(*this, other);
    }

    GrowingHashmap& operator=(GrowingHashmap other)
    {
        swap(*this, other);
        return *this;
    }

    friend void swap(GrowingHashmap& first, GrowingHashmap& second) noexcept
    {
        std::swap(first.used, second.used);
        std::swap(first.fill, second.fill);
        std::swap(first.mask, second.mask);
        std::swap(first.m_map, second.m_map);
    }

    size_type size() const
    {
        return used;
    }
    size_type capacity() const
    {
        return mask + 1;
    }
    bool empty() const
    {
        return used == 0;
    }

    value_type get(key_type key) const noexcept
    {
        if (m_map == nullptr) return value_type();

        return m_map[lookup(key)].value;
    }

    value_type& operator[](key_type key) noexcept
    {
        if (m_map == nullptr) allocate();

        size_t i = lookup(key);

        if (m_map[i].value == value_type()) {
            /* resize when 2/3 full */
            if (++fill * 3 >= (mask + 1) * 2) {
                grow((used + 1) * 2);
                i = lookup(key);
            }

            used++;
        }

        m_map[i].key = key;
        return m_map[i].value;
    }

private:
    void allocate()
    {
        mask = min_size - 1;
        m_map = new MapElem[min_size];
    }

    /**
     * lookup key inside the hashmap using a similar collision resolution
     * strategy to CPython and Ruby
     */
    size_t lookup(key_type key) const
    {
        size_t hash = static_cast<size_t>(key);
        size_t i = hash & static_cast<size_t>(mask);

        if (m_map[i].value == value_type() || m_map[i].key == key) return i;

        size_t perturb = hash;
        while (true) {
            i = (i * 5 + perturb + 1) & static_cast<size_t>(mask);
            if (m_map[i].value == value_type() || m_map[i].key == key) return i;

            perturb >>= 5;
        }
    }

    void grow(int minUsed)
    {
        int newSize = mask + 1;
        while (newSize <= minUsed)
            newSize <<= 1;

        MapElem* oldMap = m_map;
        m_map = new MapElem[static_cast<size_t>(newSize)];

        fill = used;
        mask = newSize - 1;

        for (int i = 0; used > 0; i++)
            if (oldMap[i].value != value_type()) {
                size_t j = lookup(oldMap[i].key);

                m_map[j].key = oldMap[i].key;
                m_map[j].value = oldMap[i].value;
                used--;
            }

        used = fill;
        delete[] oldMap;
    }
};

template <typename T_Key, typename T_Entry>
struct HybridGrowingHashmap {
    using key_type = T_Key;
    using value_type = T_Entry;

    HybridGrowingHashmap()
    {
        m_extendedAscii.fill(value_type());
    }

    value_type get(char key) const noexcept
    {
        /** treat char as value between 0 and 127 for performance reasons */
        return m_extendedAscii[static_cast<uint8_t>(key)];
    }

    template <typename CharT>
    value_type get(CharT key) const noexcept
    {
        if (key >= 0 && key <= 255)
            return m_extendedAscii[static_cast<uint8_t>(key)];
        else
            return m_map.get(static_cast<key_type>(key));
    }

    value_type& operator[](char key) noexcept
    {
        /** treat char as value between 0 and 127 for performance reasons */
        return m_extendedAscii[static_cast<uint8_t>(key)];
    }

    template <typename CharT>
    value_type& operator[](CharT key)
    {
        if (key >= 0 && key <= 255)
            return m_extendedAscii[static_cast<uint8_t>(key)];
        else
            return m_map[static_cast<key_type>(key)];
    }

private:
    GrowingHashmap<key_type, value_type> m_map;
    std::array<value_type, 256> m_extendedAscii;
};

} // namespace duckdb_rapidfuzz::detail