/* SPDX-License-Identifier: MIT */
/* Copyright (c) 2022 Max Bachmann */

#pragma once

#include <cassert>
#include <cstddef>
#include <iterator>
#include <limits>
#include <ostream>
#include <stdexcept>
#include <stdint.h>
#include <sys/types.h>
#include <vector>

namespace duckdb_rapidfuzz::detail {

static inline void assume(bool b)
{
#if defined(__clang__)
    __builtin_assume(b);
#elif defined(__GNUC__) || defined(__GNUG__)
    if (!b) __builtin_unreachable();
#elif defined(_MSC_VER)
    __assume(b);
#endif
}

template <typename CharT>
CharT* to_begin(CharT* s)
{
    return s;
}

template <typename T>
auto to_begin(T& x)
{
    using std::begin;
    return begin(x);
}

template <typename CharT>
CharT* to_end(CharT* s)
{
    assume(s != nullptr);
    while (*s != 0)
        ++s;

    return s;
}

template <typename T>
auto to_end(T& x)
{
    using std::end;
    return end(x);
}

template <typename Iter>
class Range {
    Iter _first;
    Iter _last;
    // todo we might not want to cache the size for iterators
    // that can can retrieve the size in O(1) time
    size_t _size;

public:
    using value_type = typename std::iterator_traits<Iter>::value_type;
    using iterator = Iter;
    using reverse_iterator = std::reverse_iterator<iterator>;

    constexpr Range(Iter first, Iter last) : _first(first), _last(last)
    {
        assert(std::distance(_first, _last) >= 0);
        _size = static_cast<size_t>(std::distance(_first, _last));
    }

    constexpr Range(Iter first, Iter last, size_t size) : _first(first), _last(last), _size(size)
    {}

    template <typename T>
    constexpr Range(T& x) : _first(to_begin(x)), _last(to_end(x))
    {
        assert(std::distance(_first, _last) >= 0);
        _size = static_cast<size_t>(std::distance(_first, _last));
    }

    constexpr iterator begin() const noexcept
    {
        return _first;
    }
    constexpr iterator end() const noexcept
    {
        return _last;
    }

    constexpr reverse_iterator rbegin() const noexcept
    {
        return reverse_iterator(end());
    }
    constexpr reverse_iterator rend() const noexcept
    {
        return reverse_iterator(begin());
    }

    constexpr size_t size() const
    {
        return _size;
    }

    constexpr bool empty() const
    {
        return size() == 0;
    }
    explicit constexpr operator bool() const
    {
        return !empty();
    }

    template <
        typename... Dummy, typename IterCopy = Iter,
        typename = std::enable_if_t<std::is_base_of_v<
            std::random_access_iterator_tag, typename std::iterator_traits<IterCopy>::iterator_category>>>
    constexpr decltype(auto) operator[](size_t n) const
    {
        return _first[static_cast<ptrdiff_t>(n)];
    }

    constexpr void remove_prefix(size_t n)
    {
        if constexpr (std::is_base_of_v<std::random_access_iterator_tag,
                                        typename std::iterator_traits<Iter>::iterator_category>)
            _first += static_cast<ptrdiff_t>(n);
        else
            for (size_t i = 0; i < n; ++i)
                _first++;

        _size -= n;
    }
    constexpr void remove_suffix(size_t n)
    {
        if constexpr (std::is_base_of_v<std::random_access_iterator_tag,
                                        typename std::iterator_traits<Iter>::iterator_category>)
            _last -= static_cast<ptrdiff_t>(n);
        else
            for (size_t i = 0; i < n; ++i)
                _last--;

        _size -= n;
    }

    constexpr Range subseq(size_t pos = 0, size_t count = std::numeric_limits<size_t>::max())
    {
        if (pos > size()) throw std::out_of_range("Index out of range in Range::substr");

        Range res = *this;
        res.remove_prefix(pos);
        if (count < res.size()) res.remove_suffix(res.size() - count);

        return res;
    }

    constexpr decltype(auto) front() const
    {
        return *(_first);
    }

    constexpr decltype(auto) back() const
    {
        return *(_last - 1);
    }

    constexpr Range<reverse_iterator> reversed() const
    {
        return {rbegin(), rend(), _size};
    }

    friend std::ostream& operator<<(std::ostream& os, const Range& seq)
    {
        os << "[";
        for (auto x : seq)
            os << static_cast<uint64_t>(x) << ", ";
        os << "]";
        return os;
    }
};

template <typename T>
Range(T& x) -> Range<decltype(to_begin(x))>;

template <typename InputIt1, typename InputIt2>
inline bool operator==(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    return std::equal(a.begin(), a.end(), b.begin(), b.end());
}

template <typename InputIt1, typename InputIt2>
inline bool operator!=(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    return !(a == b);
}

template <typename InputIt1, typename InputIt2>
inline bool operator<(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    return (std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end()));
}

template <typename InputIt1, typename InputIt2>
inline bool operator>(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    return b < a;
}

template <typename InputIt1, typename InputIt2>
inline bool operator<=(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    return !(b < a);
}

template <typename InputIt1, typename InputIt2>
inline bool operator>=(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    return !(a < b);
}

template <typename InputIt>
using RangeVec = std::vector<Range<InputIt>>;

} // namespace duckdb_rapidfuzz::detail
