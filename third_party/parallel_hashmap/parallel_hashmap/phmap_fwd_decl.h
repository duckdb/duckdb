#if !defined(phmap_fwd_decl_h_guard_)
#define phmap_fwd_decl_h_guard_

// ---------------------------------------------------------------------------
// Copyright (c) 2019, Gregory Popovitch - greg7mdp@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
// ---------------------------------------------------------------------------

#ifdef _MSC_VER
    #pragma warning(push)  
    #pragma warning(disable : 4514) // unreferenced inline function has been removed
    #pragma warning(disable : 4710) // function not inlined
    #pragma warning(disable : 4711) // selected for automatic inline expansion
#endif

#include <memory>
#include <utility>
#include <mutex>

#if defined(PHMAP_USE_ABSL_HASH) && !defined(ABSL_HASH_HASH_H_)
    namespace absl { template <class T> struct Hash; };
#endif

namespace phmap {

#if defined(PHMAP_USE_ABSL_HASH)
    template <class T> using Hash = ::absl::Hash<T>;
#else
    template <class T> struct Hash;
#endif

    template <class T> struct EqualTo;
    template <class T> struct Less;
    template <class T> using Allocator      = typename std::allocator<T>;
    template<class T1, class T2> using Pair = typename std::pair<T1, T2>;

    class NullMutex;

    namespace priv {

        // The hash of an object of type T is computed by using phmap::Hash.
        template <class T, class E = void>
        struct HashEq 
        {
            using Hash = phmap::Hash<T>;
            using Eq   = phmap::EqualTo<T>;
        };

        template <class T>
        using hash_default_hash = typename priv::HashEq<T>::Hash;

        template <class T>
        using hash_default_eq = typename priv::HashEq<T>::Eq;

        // type alias for std::allocator so we can forward declare without including other headers
        template <class T>  
        using Allocator = typename phmap::Allocator<T>;

        // type alias for std::pair so we can forward declare without including other headers
        template<class T1, class T2> 
        using Pair = typename phmap::Pair<T1, T2>;

    }  // namespace priv

    // ------------- forward declarations for hash containers ----------------------------------
    template <class T, 
              class Hash  = phmap::priv::hash_default_hash<T>,
              class Eq    = phmap::priv::hash_default_eq<T>,
              class Alloc = phmap::priv::Allocator<T>>  // alias for std::allocator
        class flat_hash_set;

    template <class K, class V,
              class Hash  = phmap::priv::hash_default_hash<K>,
              class Eq    = phmap::priv::hash_default_eq<K>,
              class Alloc = phmap::priv::Allocator<
                            phmap::priv::Pair<const K, V>>> // alias for std::allocator
        class flat_hash_map;
    
    template <class T, 
              class Hash  = phmap::priv::hash_default_hash<T>,
              class Eq    = phmap::priv::hash_default_eq<T>,
              class Alloc = phmap::priv::Allocator<T>> // alias for std::allocator
        class node_hash_set;

    template <class Key, class Value,
              class Hash  = phmap::priv::hash_default_hash<Key>,
              class Eq    = phmap::priv::hash_default_eq<Key>,
              class Alloc = phmap::priv::Allocator<
                            phmap::priv::Pair<const Key, Value>>> // alias for std::allocator
        class node_hash_map;

    template <class T,
              class Hash  = phmap::priv::hash_default_hash<T>,
              class Eq    = phmap::priv::hash_default_eq<T>,
              class Alloc = phmap::priv::Allocator<T>, // alias for std::allocator
              size_t N    = 4,                  // 2**N submaps
              class Mutex = phmap::NullMutex>   // use std::mutex to enable internal locks
        class parallel_flat_hash_set;

    template <class K, class V,
              class Hash  = phmap::priv::hash_default_hash<K>,
              class Eq    = phmap::priv::hash_default_eq<K>,
              class Alloc = phmap::priv::Allocator<
                            phmap::priv::Pair<const K, V>>, // alias for std::allocator
              size_t N    = 4,                  // 2**N submaps
              class Mutex = phmap::NullMutex>   // use std::mutex to enable internal locks
        class parallel_flat_hash_map;

    template <class T, 
              class Hash  = phmap::priv::hash_default_hash<T>,
              class Eq    = phmap::priv::hash_default_eq<T>,
              class Alloc = phmap::priv::Allocator<T>, // alias for std::allocator
              size_t N    = 4,                  // 2**N submaps
              class Mutex = phmap::NullMutex>   // use std::mutex to enable internal locks
        class parallel_node_hash_set;

    template <class Key, class Value,
              class Hash  = phmap::priv::hash_default_hash<Key>,
              class Eq    = phmap::priv::hash_default_eq<Key>,
              class Alloc = phmap::priv::Allocator<
                            phmap::priv::Pair<const Key, Value>>, // alias for std::allocator
              size_t N    = 4,                  // 2**N submaps
              class Mutex = phmap::NullMutex>   // use std::mutex to enable internal locks
        class parallel_node_hash_map;

    // -----------------------------------------------------------------------------
    // phmap::parallel_*_hash_* using std::mutex by default
    // -----------------------------------------------------------------------------
    template <class T,
              class Hash  = phmap::priv::hash_default_hash<T>,
              class Eq    = phmap::priv::hash_default_eq<T>,
              class Alloc = phmap::priv::Allocator<T>,
              size_t N    = 4>
    using parallel_flat_hash_set_m = parallel_flat_hash_set<T, Hash, Eq, Alloc, N, std::mutex>;

    template <class K, class V,
              class Hash  = phmap::priv::hash_default_hash<K>,
              class Eq    = phmap::priv::hash_default_eq<K>,
              class Alloc = phmap::priv::Allocator<phmap::priv::Pair<const K, V>>,
              size_t N    = 4>
    using parallel_flat_hash_map_m = parallel_flat_hash_map<K, V, Hash, Eq, Alloc, N, std::mutex>;

    template <class T,
              class Hash  = phmap::priv::hash_default_hash<T>,
              class Eq    = phmap::priv::hash_default_eq<T>,
              class Alloc = phmap::priv::Allocator<T>,
              size_t N    = 4>
    using parallel_node_hash_set_m = parallel_node_hash_set<T, Hash, Eq, Alloc, N, std::mutex>;

    template <class K, class V,
              class Hash  = phmap::priv::hash_default_hash<K>,
              class Eq    = phmap::priv::hash_default_eq<K>,
              class Alloc = phmap::priv::Allocator<phmap::priv::Pair<const K, V>>,
              size_t N     = 4>
    using parallel_node_hash_map_m = parallel_node_hash_map<K, V, Hash, Eq, Alloc, N, std::mutex>;

    // ------------- forward declarations for btree containers ----------------------------------
    template <typename Key, typename Compare = phmap::Less<Key>,
              typename Alloc = phmap::Allocator<Key>>
        class btree_set;

    template <typename Key, typename Compare = phmap::Less<Key>,
              typename Alloc = phmap::Allocator<Key>>
        class btree_multiset;

    template <typename Key, typename Value, typename Compare = phmap::Less<Key>,
              typename Alloc = phmap::Allocator<phmap::priv::Pair<const Key, Value>>>
        class btree_map;
    
    template <typename Key, typename Value, typename Compare = phmap::Less<Key>,
              typename Alloc = phmap::Allocator<phmap::priv::Pair<const Key, Value>>>
        class btree_multimap;

}  // namespace phmap


#ifdef _MSC_VER
     #pragma warning(pop)  
#endif

#endif // phmap_fwd_decl_h_guard_
