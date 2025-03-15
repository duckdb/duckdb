#if !defined(phmap_dump_h_guard_)
#define phmap_dump_h_guard_

// ---------------------------------------------------------------------------
// Copyright (c) 2019, Gregory Popovitch - greg7mdp@gmail.com
//
//       providing dump/load/mmap_load
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

#include <iostream>
#include <fstream>
#include <functional>
#include "phmap.h"
namespace phmap
{

namespace type_traits_internal {

#if defined(__GLIBCXX__) && __GLIBCXX__ < 20150801
    template<typename T> struct IsTriviallyCopyable : public std::integral_constant<bool, __has_trivial_copy(T)> {};
#else
    template<typename T> struct IsTriviallyCopyable : public std::is_trivially_copyable<T> {};
#endif

template <class T1, class T2>
struct IsTriviallyCopyable<std::pair<T1, T2>> {
    static constexpr bool value = IsTriviallyCopyable<T1>::value && IsTriviallyCopyable<T2>::value;
};
}

namespace priv {

#if !defined(PHMAP_NON_DETERMINISTIC) && !defined(PHMAP_DISABLE_DUMP)

static constexpr size_t s_version_base = std::numeric_limits<size_t>::max() - 10;
static constexpr size_t s_version = s_version_base;
// ------------------------------------------------------------------------
// dump/load for raw_hash_set
// ------------------------------------------------------------------------
template <class Policy, class Hash, class Eq, class Alloc>
template<typename OutputArchive>
bool raw_hash_set<Policy, Hash, Eq, Alloc>::phmap_dump(OutputArchive& ar) const {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                    "value_type should be trivially copyable");

    ar.saveBinary(&s_version, sizeof(size_t));
    ar.saveBinary(&size_, sizeof(size_t));
    ar.saveBinary(&capacity_, sizeof(size_t));
    if (size_ == 0)
        return true;
    ar.saveBinary(ctrl_,  sizeof(ctrl_t) * (capacity_ + Group::kWidth + 1));
    ar.saveBinary(slots_, sizeof(slot_type) * capacity_);
    ar.saveBinary(&growth_left(), sizeof(size_t));
    return true;
}

template <class Policy, class Hash, class Eq, class Alloc>
template<typename InputArchive>
bool raw_hash_set<Policy, Hash, Eq, Alloc>::phmap_load(InputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                    "value_type should be trivially copyable");
    raw_hash_set<Policy, Hash, Eq, Alloc>().swap(*this); // clear any existing content

    size_t version = 0;
    ar.loadBinary(&version, sizeof(size_t));
    if (version < s_version_base) {
        // we didn't store the version, version actually contains the size
        size_ = version;
    } else {
        ar.loadBinary(&size_, sizeof(size_t));
    }
    ar.loadBinary(&capacity_, sizeof(size_t));

    if (capacity_) {
        // allocate memory for ctrl_ and slots_
        initialize_slots(capacity_);
    }
    if (size_ == 0)
        return true;
    ar.loadBinary(ctrl_,  sizeof(ctrl_t) * (capacity_ + Group::kWidth + 1));
    ar.loadBinary(slots_, sizeof(slot_type) * capacity_);
    if (version >= s_version_base) {
        // growth_left should be restored after calling initialize_slots() which resets it.
        ar.loadBinary(&growth_left(), sizeof(size_t));
    } else {
       drop_deletes_without_resize();
    }
    return true;
}

// ------------------------------------------------------------------------
// dump/load for parallel_hash_set
// ------------------------------------------------------------------------
template <size_t N,
          template <class, class, class, class> class RefSet,
          class Mtx_,
          class Policy, class Hash, class Eq, class Alloc>
template<typename OutputArchive>
bool parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, Eq, Alloc>::phmap_dump(OutputArchive& ar) const {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                  "value_type should be trivially copyable");

    size_t submap_count = subcnt();
    ar.saveBinary(&submap_count, sizeof(size_t));
    for (size_t i = 0; i < sets_.size(); ++i) {
        auto& inner = sets_[i];
        typename Lockable::UniqueLock m(const_cast<Inner&>(inner));
        if (!inner.set_.phmap_dump(ar)) {
            std::cerr << "Failed to dump submap " << i << std::endl;
            return false;
        }
    }
    return true;
}

template <size_t N,
          template <class, class, class, class> class RefSet,
          class Mtx_,
          class Policy, class Hash, class Eq, class Alloc>
template<typename InputArchive>
bool parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, Eq, Alloc>::phmap_load(InputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                  "value_type should be trivially copyable");

    size_t submap_count = 0;
    ar.loadBinary(&submap_count, sizeof(size_t));
    if (submap_count != subcnt()) {
        std::cerr << "submap count(" << submap_count << ") != N(" << N << ")" << std::endl;
        return false;
    }

    for (size_t i = 0; i < submap_count; ++i) {            
        auto& inner = sets_[i];
        typename Lockable::UniqueLock m(const_cast<Inner&>(inner));
        if (!inner.set_.phmap_load(ar)) {
            std::cerr << "Failed to load submap " << i << std::endl;
            return false;
        }
    }
    return true;
}

#endif // !defined(PHMAP_NON_DETERMINISTIC) && !defined(PHMAP_DISABLE_DUMP)

} // namespace priv



// ------------------------------------------------------------------------
// BinaryArchive
//       File is closed when archive object is destroyed
// ------------------------------------------------------------------------

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------
class BinaryOutputArchive {
public:
    BinaryOutputArchive(const char *file_path) {
      os_ = new std::ofstream(file_path, std::ofstream::out |
                                             std::ofstream::trunc |
                                             std::ofstream::binary);
      destruct_ = [this]() { delete os_; };
    }

    BinaryOutputArchive(std::ostream &os) : os_(&os) {}

    ~BinaryOutputArchive() {
        if (destruct_) {
            destruct_();
        }
    }
    
    BinaryOutputArchive(const BinaryOutputArchive&) = delete;
    BinaryOutputArchive& operator=(const BinaryOutputArchive&) = delete;

    bool saveBinary(const void *p, size_t sz) {
        os_->write(reinterpret_cast<const char*>(p), (std::streamsize)sz);
        return true;
    }

    template<typename V>
    typename std::enable_if<type_traits_internal::IsTriviallyCopyable<V>::value, bool>::type
    saveBinary(const V& v) {
        os_->write(reinterpret_cast<const char *>(&v), sizeof(V));
        return true;
    }

    template<typename Map>
    auto saveBinary(const Map& v) -> decltype(v.phmap_dump(*this), bool())
    {
        return v.phmap_dump(*this);
    }

private:
    std::ostream* os_;
    std::function<void()> destruct_;
};


class BinaryInputArchive {
public:
    BinaryInputArchive(const char * file_path) {
      is_ = new std::ifstream(file_path,
                              std::ifstream::in | std::ifstream::binary);
      destruct_ = [this]() { delete is_; };
    }

    BinaryInputArchive(std::istream& is) : is_(&is) {}
    
    ~BinaryInputArchive() {
        if (destruct_) {
            destruct_();
        }
    }

    BinaryInputArchive(const BinaryInputArchive&) = delete;
    BinaryInputArchive& operator=(const BinaryInputArchive&) = delete;

    bool loadBinary(void* p, size_t sz) {
        is_->read(reinterpret_cast<char*>(p),  (std::streamsize)sz);
        return true;
    }

    template<typename V>
    typename std::enable_if<type_traits_internal::IsTriviallyCopyable<V>::value, bool>::type
    loadBinary(V* v) {
        is_->read(reinterpret_cast<char *>(v), sizeof(V));
        return true;
    }

    template<typename Map>
    auto loadBinary(Map* v) -> decltype(v->phmap_load(*this), bool())
    {
        return v->phmap_load(*this);
    }
    
private:
    std::istream* is_;
    std::function<void()> destruct_;
};

} // namespace phmap


#ifdef CEREAL_SIZE_TYPE

template <class T>
using PhmapTrivCopyable = typename phmap::type_traits_internal::IsTriviallyCopyable<T>;
    
namespace cereal
{
    // Overload Cereal serialization code for phmap::flat_hash_map
    // -----------------------------------------------------------
    template <class K, class V, class Hash, class Eq, class A>
    void save(typename std::enable_if<PhmapTrivCopyable<K>::value && PhmapTrivCopyable<V>::value, typename cereal::BinaryOutputArchive>::type &ar,
              phmap::flat_hash_map<K, V, Hash, Eq, A> const &hmap)
    {
        hmap.phmap_dump(ar);
    }

    template <class K, class V, class Hash, class Eq, class A>
    void load(typename std::enable_if<PhmapTrivCopyable<K>::value && PhmapTrivCopyable<V>::value, typename cereal::BinaryInputArchive>::type &ar, 
              phmap::flat_hash_map<K, V, Hash, Eq, A>  &hmap)
    {
        hmap.phmap_load(ar);
    }


    // Overload Cereal serialization code for phmap::parallel_flat_hash_map
    // --------------------------------------------------------------------
    template <class K, class V, class Hash, class Eq, class A, size_t N, class Mtx_>
    void save(typename std::enable_if<PhmapTrivCopyable<K>::value && PhmapTrivCopyable<V>::value, typename cereal::BinaryOutputArchive>::type &ar,
              phmap::parallel_flat_hash_map<K, V, Hash, Eq, A, N, Mtx_> const &hmap)
    {
        hmap.phmap_dump(ar);
    }

    template <class K, class V, class Hash, class Eq, class A, size_t N, class Mtx_>
    void load(typename std::enable_if<PhmapTrivCopyable<K>::value && PhmapTrivCopyable<V>::value, typename cereal::BinaryInputArchive>::type &ar, 
              phmap::parallel_flat_hash_map<K, V, Hash, Eq, A, N, Mtx_>  &hmap)
    {
        hmap.phmap_load(ar);
    }

    // Overload Cereal serialization code for phmap::flat_hash_set
    // -----------------------------------------------------------
    template <class K, class Hash, class Eq, class A>
    void save(typename std::enable_if<PhmapTrivCopyable<K>::value, typename cereal::BinaryOutputArchive>::type &ar,
              phmap::flat_hash_set<K, Hash, Eq, A> const &hset)
    {
        hset.phmap_dump(ar);
    }

    template <class K, class Hash, class Eq, class A>
    void load(typename std::enable_if<PhmapTrivCopyable<K>::value, typename cereal::BinaryInputArchive>::type &ar, 
              phmap::flat_hash_set<K, Hash, Eq, A>  &hset)
    {
        hset.phmap_load(ar);
    }

    // Overload Cereal serialization code for phmap::parallel_flat_hash_set
    // --------------------------------------------------------------------
    template <class K, class Hash, class Eq, class A, size_t N, class Mtx_>
    void save(typename std::enable_if<PhmapTrivCopyable<K>::value, typename cereal::BinaryOutputArchive>::type &ar,
              phmap::parallel_flat_hash_set<K, Hash, Eq, A, N, Mtx_> const &hset)
    {
        hset.phmap_dump(ar);
    }

    template <class K, class Hash, class Eq, class A, size_t N, class Mtx_>
    void load(typename std::enable_if<PhmapTrivCopyable<K>::value, typename cereal::BinaryInputArchive>::type &ar, 
              phmap::parallel_flat_hash_set<K, Hash, Eq, A, N, Mtx_>  &hset)
    {
        hset.phmap_load(ar);
    }
}

#endif




#endif // phmap_dump_h_guard_
