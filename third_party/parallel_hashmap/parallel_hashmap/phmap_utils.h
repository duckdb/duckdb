#if !defined(phmap_utils_h_guard_)
#define phmap_utils_h_guard_

// ---------------------------------------------------------------------------
// Copyright (c) 2019, Gregory Popovitch - greg7mdp@gmail.com
//
//       minimal header providing phmap::HashState
//
//       use as:  phmap::HashState().combine(0, _first_name, _last_name, _age);
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ---------------------------------------------------------------------------

#ifdef _MSC_VER
    #pragma warning(push)  
    #pragma warning(disable : 4514) // unreferenced inline function has been removed
    #pragma warning(disable : 4710) // function not inlined
    #pragma warning(disable : 4711) // selected for automatic inline expansion
#endif

#include <cstdint>
#include <functional>
#include <tuple>
#include "phmap_bits.h"

// ---------------------------------------------------------------
// Absl forward declaration requires global scope.
// ---------------------------------------------------------------
#if defined(PHMAP_USE_ABSL_HASH) && !defined(phmap_fwd_decl_h_guard_) && !defined(ABSL_HASH_HASH_H_)
    namespace absl { template <class T> struct Hash; };
#endif

namespace phmap
{

// ---------------------------------------------------------------
// ---------------------------------------------------------------
template<int n> 
struct phmap_mix
{
    inline size_t operator()(size_t) const;
};

template<>
struct phmap_mix<4>
{
    inline size_t operator()(size_t a) const
    {
        static constexpr uint64_t kmul = 0xcc9e2d51UL;
        uint64_t l = a * kmul;
        return static_cast<size_t>(l ^ (l >> 32));
    }
};

#if defined(PHMAP_HAS_UMUL128)
    template<>
    struct phmap_mix<8>
    {
        // Very fast mixing (similar to Abseil)
        inline size_t operator()(size_t a) const
        {
            static constexpr uint64_t k = 0xde5fb9d2630458e9ULL;
            uint64_t h;
            uint64_t l = umul128(a, k, &h);
            return static_cast<size_t>(h + l);
        }
    };
#else
    template<>
    struct phmap_mix<8>
    {
        inline size_t operator()(size_t a) const
        {
            a = (~a) + (a << 21); // a = (a << 21) - a - 1;
            a = a ^ (a >> 24);
            a = (a + (a << 3)) + (a << 8); // a * 265
            a = a ^ (a >> 14);
            a = (a + (a << 2)) + (a << 4); // a * 21
            a = a ^ (a >> 28);
            a = a + (a << 31);
            return static_cast<size_t>(a);
        }
    };
#endif

// --------------------------------------------
template<int n> 
struct fold_if_needed
{
    inline size_t operator()(uint64_t) const;
};

template<>
struct fold_if_needed<4>
{
    inline size_t operator()(uint64_t a) const
    {
        return static_cast<size_t>(a ^ (a >> 32));
    }
};

template<>
struct fold_if_needed<8>
{
    inline size_t operator()(uint64_t a) const
    {
        return static_cast<size_t>(a);
    }
};

// ---------------------------------------------------------------
// see if class T has a hash_value() friend method
// ---------------------------------------------------------------
template<typename T>
struct has_hash_value
{
private:
    typedef std::true_type yes;
    typedef std::false_type no;

    template<typename U> static auto test(int) -> decltype(hash_value(std::declval<const U&>()) == 1, yes());

    template<typename> static no test(...);

public:
    static constexpr bool value = std::is_same<decltype(test<T>(0)), yes>::value;
};

#if defined(PHMAP_USE_ABSL_HASH) && !defined(phmap_fwd_decl_h_guard_)
    template <class T> using Hash = ::absl::Hash<T>;
#elif !defined(PHMAP_USE_ABSL_HASH)
// ---------------------------------------------------------------
//               phmap::Hash
// ---------------------------------------------------------------
template <class T>
struct Hash
{
    template <class U, typename std::enable_if<has_hash_value<U>::value, int>::type = 0>
    size_t _hash(const T& val) const
    {
        return hash_value(val);
    }
 
    template <class U, typename std::enable_if<!has_hash_value<U>::value, int>::type = 0>
    size_t _hash(const T& val) const
    {
        return std::hash<T>()(val);
    }
 
    inline size_t operator()(const T& val) const
    {
        return _hash<T>(val);
    }
};
 
template<class ArgumentType, class ResultType>
struct phmap_unary_function
{
    typedef ArgumentType argument_type;
    typedef ResultType result_type;
};

template <>
struct Hash<bool> : public phmap_unary_function<bool, size_t>
{
    inline size_t operator()(bool val) const noexcept
    { return static_cast<size_t>(val); }
};

template <>
struct Hash<char> : public phmap_unary_function<char, size_t>
{
    inline size_t operator()(char val) const noexcept
    { return static_cast<size_t>(val); }
};

template <>
struct Hash<signed char> : public phmap_unary_function<signed char, size_t>
{
    inline size_t operator()(signed char val) const noexcept
    { return static_cast<size_t>(val); }
};

template <>
struct Hash<unsigned char> : public phmap_unary_function<unsigned char, size_t>
{
    inline size_t operator()(unsigned char val) const noexcept
    { return static_cast<size_t>(val); }
};

#ifdef PHMAP_HAS_NATIVE_WCHAR_T
template <>
struct Hash<wchar_t> : public phmap_unary_function<wchar_t, size_t>
{
    inline size_t operator()(wchar_t val) const noexcept
    { return static_cast<size_t>(val); }
};
#endif

template <>
struct Hash<int16_t> : public phmap_unary_function<int16_t, size_t>
{
    inline size_t operator()(int16_t val) const noexcept
    { return static_cast<size_t>(val); }
};

template <>
struct Hash<uint16_t> : public phmap_unary_function<uint16_t, size_t>
{
    inline size_t operator()(uint16_t val) const noexcept
    { return static_cast<size_t>(val); }
};

template <>
struct Hash<int32_t> : public phmap_unary_function<int32_t, size_t>
{
    inline size_t operator()(int32_t val) const noexcept
    { return static_cast<size_t>(val); }
};

template <>
struct Hash<uint32_t> : public phmap_unary_function<uint32_t, size_t>
{
    inline size_t operator()(uint32_t val) const noexcept
    { return static_cast<size_t>(val); }
};

template <>
struct Hash<int64_t> : public phmap_unary_function<int64_t, size_t>
{
    inline size_t operator()(int64_t val) const noexcept
    { return fold_if_needed<sizeof(size_t)>()(static_cast<uint64_t>(val)); }
};

template <>
struct Hash<uint64_t> : public phmap_unary_function<uint64_t, size_t>
{
    inline size_t operator()(uint64_t val) const noexcept
    { return fold_if_needed<sizeof(size_t)>()(val); }
};

template <>
struct Hash<float> : public phmap_unary_function<float, size_t>
{
    inline size_t operator()(float val) const noexcept
    {
        // -0.0 and 0.0 should return same hash
        uint32_t *as_int = reinterpret_cast<uint32_t *>(&val);
        return (val == 0) ? static_cast<size_t>(0) : 
                            static_cast<size_t>(*as_int);
    }
};

template <>
struct Hash<double> : public phmap_unary_function<double, size_t>
{
    inline size_t operator()(double val) const noexcept
    {
        // -0.0 and 0.0 should return same hash
        uint64_t *as_int = reinterpret_cast<uint64_t *>(&val);
        return (val == 0) ? static_cast<size_t>(0) : 
                            fold_if_needed<sizeof(size_t)>()(*as_int);
    }
};

#endif

#if defined(_MSC_VER)
#   define PHMAP_HASH_ROTL32(x, r) _rotl(x,r)
#else
#   define PHMAP_HASH_ROTL32(x, r) (x << r) | (x >> (32 - r))
#endif


template <class H, int sz> struct Combiner
{
    H operator()(H seed, size_t value);
};

template <class H> struct Combiner<H, 4>
{
    H operator()(H h1, size_t k1)
    {
        // Copyright 2005-2014 Daniel James.
        // Distributed under the Boost Software License, Version 1.0. (See accompanying
        // file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
        
        const uint32_t c1 = 0xcc9e2d51;
        const uint32_t c2 = 0x1b873593;

        k1 *= c1;
        k1 = PHMAP_HASH_ROTL32(k1,15);
        k1 *= c2;

        h1 ^= k1;
        h1 = PHMAP_HASH_ROTL32(h1,13);
        h1 = h1*5+0xe6546b64;

        return h1;
    }
};

template <class H> struct Combiner<H, 8>
{
    H operator()(H h, size_t k)
    {
        // Copyright 2005-2014 Daniel James.
        // Distributed under the Boost Software License, Version 1.0. (See accompanying
        // file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
        const uint64_t m = (uint64_t(0xc6a4a793) << 32) + 0x5bd1e995;
        const int r = 47;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;

        // Completely arbitrary number, to prevent 0's
        // from hashing to 0.
        h += 0xe6546b64;

        return h;
    }
};

// define HashState to combine member hashes... see example below
// -----------------------------------------------------------------------------
template <typename H>
class HashStateBase {
public:
    template <typename T, typename... Ts>
    static H combine(H state, const T& value, const Ts&... values);

    static H combine(H state) { return state; }
};

template <typename H>
template <typename T, typename... Ts>
H HashStateBase<H>::combine(H seed, const T& v, const Ts&... vs)
{
    return HashStateBase<H>::combine(Combiner<H, sizeof(H)>()(
                                         seed, phmap::Hash<T>()(v)), 
                                     vs...);
}

using HashState = HashStateBase<size_t>;

// -----------------------------------------------------------------------------

#if !defined(PHMAP_USE_ABSL_HASH)

// define Hash for std::pair
// -------------------------
template<class T1, class T2> 
struct Hash<std::pair<T1, T2>> {
    size_t operator()(std::pair<T1, T2> const& p) const noexcept {
        return phmap::HashState().combine(phmap::Hash<T1>()(p.first), p.second);
    }
};

// define Hash for std::tuple
// --------------------------
template<class... T> 
struct Hash<std::tuple<T...>> {
    size_t operator()(std::tuple<T...> const& t) const noexcept {
        size_t seed = 0;
        return _hash_helper(seed, t);
    }

private:
    template<size_t I = 0, class TUP>
    typename std::enable_if<I == std::tuple_size<TUP>::value, size_t>::type
    _hash_helper(size_t seed, const TUP &) const noexcept { return seed; }

    template<size_t I = 0, class TUP>
    typename std::enable_if<I < std::tuple_size<TUP>::value, size_t>::type
    _hash_helper(size_t seed, const TUP &t) const noexcept {
        const auto &el = std::get<I>(t);
        using el_type = typename std::remove_cv<typename std::remove_reference<decltype(el)>::type>::type;
        seed = Combiner<size_t, sizeof(size_t)>()(seed, phmap::Hash<el_type>()(el));
        return _hash_helper<I + 1>(seed, t);
    }
};


#endif


}  // namespace phmap

#ifdef _MSC_VER
     #pragma warning(pop)  
#endif

#endif // phmap_utils_h_guard_
