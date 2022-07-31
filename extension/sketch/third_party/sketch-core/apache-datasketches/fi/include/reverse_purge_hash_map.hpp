/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef REVERSE_PURGE_HASH_MAP_HPP_
#define REVERSE_PURGE_HASH_MAP_HPP_

#include <memory>
#include <iterator>

namespace datasketches {

/*
 * This is a specialized linear-probing hash map with a reverse purge operation
 * that removes all entries in the map with values that are less than zero.
 * Based on Java implementation here:
 * https://github.com/DataSketches/sketches-core/blob/master/src/main/java/com/yahoo/sketches/frequencies/ReversePurgeItemHashMap.java
 * author Alexander Saydakov
 */

template<typename K, typename V = uint64_t, typename H = std::hash<K>, typename E = std::equal_to<K>, typename A = std::allocator<K>>
class reverse_purge_hash_map {
public:
  using AllocV = typename std::allocator_traits<A>::template rebind_alloc<V>;
  using AllocU16 = typename std::allocator_traits<A>::template rebind_alloc<uint16_t>;

  reverse_purge_hash_map(uint8_t lg_size, uint8_t lg_max_size, const A& allocator);
  reverse_purge_hash_map(const reverse_purge_hash_map& other);
  reverse_purge_hash_map(reverse_purge_hash_map&& other) noexcept;
  ~reverse_purge_hash_map();
  reverse_purge_hash_map& operator=(reverse_purge_hash_map other);
  reverse_purge_hash_map& operator=(reverse_purge_hash_map&& other);

  template<typename FwdK>
  V adjust_or_insert(FwdK&& key, V value);

  V get(const K& key) const;
  uint8_t get_lg_cur_size() const;
  uint8_t get_lg_max_size() const;
  uint32_t get_capacity() const;
  uint32_t get_num_active() const;
  const A& get_allocator() const;

  class iterator;
  iterator begin() const;
  iterator end() const;

private:
  static constexpr double LOAD_FACTOR = 0.75;
  static constexpr uint16_t DRIFT_LIMIT = 1024; // used only for stress testing
  static constexpr uint32_t MAX_SAMPLE_SIZE = 1024; // number of samples to compute approximate median during purge

  A allocator_;
  uint8_t lg_cur_size_;
  uint8_t lg_max_size_;
  uint32_t num_active_;
  K* keys_;
  V* values_;
  uint16_t* states_;

  inline bool is_active(uint32_t probe) const;
  void subtract_and_keep_positive_only(V amount);
  void hash_delete(uint32_t probe);
  uint32_t internal_adjust_or_insert(const K& key, V value);
  V resize_or_purge_if_needed();
  void resize(uint8_t lg_new_size);
  V purge();
};

// This iterator uses strides based on golden ratio to avoid clustering during merge
template<typename K, typename V, typename H, typename E, typename A>
class reverse_purge_hash_map<K, V, H, E, A>::iterator: public std::iterator<std::input_iterator_tag, K> {
public:
  friend class reverse_purge_hash_map<K, V, H, E, A>;
  iterator& operator++() {
    ++count;
    if (count < map->num_active_) {
      const uint32_t mask = (1 << map->lg_cur_size_) - 1;
      do {
        index = (index + stride) & mask;
      } while (!map->is_active(index));
    }
    return *this;
  }
  iterator operator++(int) { iterator tmp(*this); operator++(); return tmp; }
  bool operator==(const iterator& rhs) const { return count == rhs.count; }
  bool operator!=(const iterator& rhs) const { return count != rhs.count; }
  const std::pair<K&, V> operator*() const {
    return std::pair<K&, V>(map->keys_[index], map->values_[index]);
  }
private:
  static constexpr double GOLDEN_RATIO_RECIPROCAL = 0.6180339887498949; // = (sqrt(5) - 1) / 2
  const reverse_purge_hash_map<K, V, H, E, A>* map;
  uint32_t index;
  uint32_t count;
  uint32_t stride;
  iterator(const reverse_purge_hash_map<K, V, H, E, A>* map, uint32_t index, uint32_t count):
    map(map), index(index), count(count), stride(static_cast<uint32_t>((1 << map->lg_cur_size_) * GOLDEN_RATIO_RECIPROCAL) | 1) {}
};

} /* namespace datasketches */

#include "reverse_purge_hash_map_impl.hpp"

#endif
