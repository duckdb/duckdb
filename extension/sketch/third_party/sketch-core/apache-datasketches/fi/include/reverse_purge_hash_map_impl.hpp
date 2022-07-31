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

#ifndef REVERSE_PURGE_HASH_MAP_IMPL_HPP_
#define REVERSE_PURGE_HASH_MAP_IMPL_HPP_

#include <memory>
#include <algorithm>
#include <iterator>
#include <cmath>

#include "MurmurHash3.h"

namespace datasketches {

// clang++ seems to require this declaration for CMAKE_BUILD_TYPE='Debug"
template<typename K, typename V, typename H, typename E, typename A>
constexpr uint32_t reverse_purge_hash_map<K, V, H, E, A>::MAX_SAMPLE_SIZE;

template<typename K, typename V, typename H, typename E, typename A>
reverse_purge_hash_map<K, V, H, E, A>::reverse_purge_hash_map(uint8_t lg_cur_size, uint8_t lg_max_size, const A& allocator):
allocator_(allocator),
lg_cur_size_(lg_cur_size),
lg_max_size_(lg_max_size),
num_active_(0),
keys_(allocator_.allocate(1ULL << lg_cur_size)),
values_(nullptr),
states_(nullptr)
{
  AllocV av(allocator_);
  values_ = av.allocate(1ULL << lg_cur_size);
  AllocU16 au16(allocator_);
  states_ = au16.allocate(1ULL << lg_cur_size);
  std::fill(states_, states_ + (1ULL << lg_cur_size), static_cast<uint16_t>(0));
}

template<typename K, typename V, typename H, typename E, typename A>
reverse_purge_hash_map<K, V, H, E, A>::reverse_purge_hash_map(const reverse_purge_hash_map<K, V, H, E, A>& other):
allocator_(other.allocator_),
lg_cur_size_(other.lg_cur_size_),
lg_max_size_(other.lg_max_size_),
num_active_(other.num_active_),
keys_(allocator_.allocate(1ULL << lg_cur_size_)),
values_(nullptr),
states_(nullptr)
{
  AllocV av(allocator_);
  values_ = av.allocate(1ULL << lg_cur_size_);
  AllocU16 au16(allocator_);
  states_ = au16.allocate(1ULL << lg_cur_size_);
  const uint32_t size = 1 << lg_cur_size_;
  if (num_active_ > 0) {
    auto num = num_active_;
    for (uint32_t i = 0; i < size; i++) {
      if (other.states_[i] > 0) {
        new (&keys_[i]) K(other.keys_[i]);
        values_[i] = other.values_[i];
      }
      if (--num == 0) break;
    }
  }
  std::copy(other.states_, other.states_ + size, states_);
}

template<typename K, typename V, typename H, typename E, typename A>
reverse_purge_hash_map<K, V, H, E, A>::reverse_purge_hash_map(reverse_purge_hash_map<K, V, H, E, A>&& other) noexcept:
allocator_(std::move(other.allocator_)),
lg_cur_size_(other.lg_cur_size_),
lg_max_size_(other.lg_max_size_),
num_active_(other.num_active_),
keys_(nullptr),
values_(nullptr),
states_(nullptr)
{
  std::swap(keys_, other.keys_);
  std::swap(values_, other.values_);
  std::swap(states_, other.states_);
  other.num_active_ = 0;
}

template<typename K, typename V, typename H, typename E, typename A>
reverse_purge_hash_map<K, V, H, E, A>::~reverse_purge_hash_map() {
  const uint32_t size = 1 << lg_cur_size_;
  if (num_active_ > 0) {
    for (uint32_t i = 0; i < size; i++) {
      if (is_active(i)) {
        keys_[i].~K();
        if (--num_active_ == 0) break;
      }
    }
  }
  if (keys_ != nullptr) {
    allocator_.deallocate(keys_, size);
  }
  if (values_ != nullptr) {
    AllocV av(allocator_);
    av.deallocate(values_, size);
  }
  if (states_ != nullptr) {
    AllocU16 au16(allocator_);
    au16.deallocate(states_, size);
  }
}

template<typename K, typename V, typename H, typename E, typename A>
reverse_purge_hash_map<K, V, H, E, A>& reverse_purge_hash_map<K, V, H, E, A>::operator=(reverse_purge_hash_map<K, V, H, E, A> other) {
  std::swap(allocator_, other.allocator_);
  std::swap(lg_cur_size_, other.lg_cur_size_);
  std::swap(lg_max_size_, other.lg_max_size_);
  std::swap(num_active_, other.num_active_);
  std::swap(keys_, other.keys_);
  std::swap(values_, other.values_);
  std::swap(states_, other.states_);
  return *this;
}

template<typename K, typename V, typename H, typename E, typename A>
reverse_purge_hash_map<K, V, H, E, A>& reverse_purge_hash_map<K, V, H, E, A>::operator=(reverse_purge_hash_map<K, V, H, E, A>&& other) {
  std::swap(allocator_, other.allocator_);
  std::swap(lg_cur_size_, other.lg_cur_size_);
  std::swap(lg_max_size_, other.lg_max_size_);
  std::swap(num_active_, other.num_active_);
  std::swap(keys_, other.keys_);
  std::swap(values_, other.values_);
  std::swap(states_, other.states_);
  return *this;
}

template<typename K, typename V, typename H, typename E, typename A>
template<typename FwdK>
V reverse_purge_hash_map<K, V, H, E, A>::adjust_or_insert(FwdK&& key, V value) {
  const uint32_t num_active_before = num_active_;
  const uint32_t index = internal_adjust_or_insert(key, value);
  if (num_active_ > num_active_before) {
    new (&keys_[index]) K(std::forward<FwdK>(key));
    return resize_or_purge_if_needed();
  }
  return 0;
}

template<typename K, typename V, typename H, typename E, typename A>
V reverse_purge_hash_map<K, V, H, E, A>::get(const K& key) const {
  const uint32_t mask = (1 << lg_cur_size_) - 1;
  uint32_t probe = fmix64(H()(key)) & mask;
  while (is_active(probe)) {
    if (E()(keys_[probe], key)) return values_[probe];
    probe = (probe + 1) & mask;
  }
  return 0;
}

template<typename K, typename V, typename H, typename E, typename A>
uint8_t reverse_purge_hash_map<K, V, H, E, A>::get_lg_cur_size() const {
  return lg_cur_size_;
}

template<typename K, typename V, typename H, typename E, typename A>
uint8_t reverse_purge_hash_map<K, V, H, E, A>::get_lg_max_size() const {
  return lg_max_size_;
}

template<typename K, typename V, typename H, typename E, typename A>
uint32_t reverse_purge_hash_map<K, V, H, E, A>::get_capacity() const {
  return static_cast<uint32_t>((1 << lg_cur_size_) * LOAD_FACTOR);
}

template<typename K, typename V, typename H, typename E, typename A>
uint32_t reverse_purge_hash_map<K, V, H, E, A>::get_num_active() const {
  return num_active_;
}

template<typename K, typename V, typename H, typename E, typename A>
const A& reverse_purge_hash_map<K, V, H, E, A>::get_allocator() const {
  return allocator_;
}

template<typename K, typename V, typename H, typename E, typename A>
typename reverse_purge_hash_map<K, V, H, E, A>::iterator reverse_purge_hash_map<K, V, H, E, A>::begin() const {
  const uint32_t size = 1 << lg_cur_size_;
  uint32_t i = 0;
  while (i < size && !is_active(i)) i++;
  return reverse_purge_hash_map<K, V, H, E, A>::iterator(this, i, 0);
}

template<typename K, typename V, typename H, typename E, typename A>
typename reverse_purge_hash_map<K, V, H, E, A>::iterator reverse_purge_hash_map<K, V, H, E, A>::end() const {
  return reverse_purge_hash_map<K, V, H, E, A>::iterator(this, 1 << lg_cur_size_, num_active_);
}

template<typename K, typename V, typename H, typename E, typename A>
bool reverse_purge_hash_map<K, V, H, E, A>::is_active(uint32_t index) const {
  return states_[index] > 0;
}

template<typename K, typename V, typename H, typename E, typename A>
void reverse_purge_hash_map<K, V, H, E, A>::subtract_and_keep_positive_only(V amount) {
  // starting from the back, find the first empty cell,
  // which establishes the high end of a cluster.
  uint32_t first_probe = (1 << lg_cur_size_) - 1;
  while (is_active(first_probe)) first_probe--;
  // when we find the next non-empty cell, we know we are at the high end of a cluster
  // work towards the front, delete any non-positive entries.
  for (uint32_t probe = first_probe; probe-- > 0;) {
    if (is_active(probe)) {
      if (values_[probe] <= amount) {
        hash_delete(probe); // does the work of deletion and moving higher items towards the front
        num_active_--;
      } else {
        values_[probe] -= amount;
      }
    }
  }
  // now work on the first cluster that was skipped
  for (uint32_t probe = (1 << lg_cur_size_); probe-- > first_probe;) {
    if (is_active(probe)) {
      if (values_[probe] <= amount) {
        hash_delete(probe);
        num_active_--;
      } else {
        values_[probe] -= amount;
      }
    }
  }
}

template<typename K, typename V, typename H, typename E, typename A>
void reverse_purge_hash_map<K, V, H, E, A>::hash_delete(uint32_t delete_index) {
  // Looks ahead in the table to search for another
  // item to move to this location
  // if none are found, the status is changed
  states_[delete_index] = 0; // mark as empty
  keys_[delete_index].~K();
  uint16_t drift = 1;
  const uint32_t mask = (1 << lg_cur_size_) - 1;
  uint32_t probe = (delete_index + drift) & mask; // map length must be a power of 2
  // advance until we find a free location replacing locations as needed
  while (is_active(probe)) {
    if (states_[probe] > drift) {
      // move current element
      new (&keys_[delete_index]) K(std::move(keys_[probe]));
      values_[delete_index] = values_[probe];
      states_[delete_index] = states_[probe] - drift;
      states_[probe] = 0; // mark as empty
      keys_[probe].~K();
      drift = 0;
      delete_index = probe;
    }
    probe = (probe + 1) & mask;
    drift++;
    // only used for theoretical analysis
    if (drift >= DRIFT_LIMIT) throw std::logic_error("drift: " + std::to_string(drift) + " >= DRIFT_LIMIT");
  }
}

template<typename K, typename V, typename H, typename E, typename A>
uint32_t reverse_purge_hash_map<K, V, H, E, A>::internal_adjust_or_insert(const K& key, V value) {
  const uint32_t mask = (1 << lg_cur_size_) - 1;
  uint32_t index = fmix64(H()(key)) & mask;
  uint16_t drift = 1;
  while (is_active(index)) {
    if (E()(keys_[index], key)) {
      // adjusting the value of an existing key
      values_[index] += value;
      return index;
    }
    index = (index + 1) & mask;
    drift++;
    // only used for theoretical analysis
    if (drift >= DRIFT_LIMIT) throw std::logic_error("drift limit reached");
  }
  // adding the key and value to the table
  if (num_active_ > get_capacity()) {
    throw std::logic_error("num_active " + std::to_string(num_active_) + " > capacity " + std::to_string(get_capacity()));
  }
  values_[index] = value;
  states_[index] = drift;
  num_active_++;
  return index;
}

template<typename K, typename V, typename H, typename E, typename A>
V reverse_purge_hash_map<K, V, H, E, A>::resize_or_purge_if_needed() {
  if (num_active_ > get_capacity()) {
    if (lg_cur_size_ < lg_max_size_) { // can grow
      resize(lg_cur_size_ + 1);
    } else { // at target size, must purge
      const V offset = purge();
      if (num_active_ > get_capacity()) {
        throw std::logic_error("purge did not reduce number of active items");
      }
      return offset;
    }
  }
  return 0;
}

template<typename K, typename V, typename H, typename E, typename A>
void reverse_purge_hash_map<K, V, H, E, A>::resize(uint8_t lg_new_size) {
  const uint32_t old_size = 1 << lg_cur_size_;
  K* old_keys = keys_;
  V* old_values = values_;
  uint16_t* old_states = states_;
  const uint32_t new_size = 1 << lg_new_size;
  keys_ = allocator_.allocate(new_size);
  AllocV av(allocator_);
  values_ = av.allocate(new_size);
  AllocU16 au16(allocator_);
  states_ = au16.allocate(new_size);
  std::fill(states_, states_ + new_size, static_cast<uint16_t>(0));
  num_active_ = 0;
  lg_cur_size_ = lg_new_size;
  for (uint32_t i = 0; i < old_size; i++) {
    if (old_states[i] > 0) {
      adjust_or_insert(std::move(old_keys[i]), old_values[i]);
      old_keys[i].~K();
    }
  }
  allocator_.deallocate(old_keys, old_size);
  av.deallocate(old_values, old_size);
  au16.deallocate(old_states, old_size);
}

template<typename K, typename V, typename H, typename E, typename A>
V reverse_purge_hash_map<K, V, H, E, A>::purge() {
  const uint32_t limit = std::min(MAX_SAMPLE_SIZE, num_active_);
  uint32_t num_samples = 0;
  uint32_t i = 0;
  AllocV av(allocator_);
  V* samples = av.allocate(limit);
  while (num_samples < limit) {
    if (is_active(i)) {
      samples[num_samples++] = values_[i];
    }
    i++;
  }
  std::nth_element(samples, samples+ (num_samples / 2), samples + num_samples);
  const V median = samples[num_samples / 2];
  av.deallocate(samples, limit);
  subtract_and_keep_positive_only(median);
  return median;
}

} /* namespace datasketches */

# endif
