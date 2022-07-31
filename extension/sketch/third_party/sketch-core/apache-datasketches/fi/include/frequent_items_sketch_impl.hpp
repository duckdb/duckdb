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

#ifndef FREQUENT_ITEMS_SKETCH_IMPL_HPP_
#define FREQUENT_ITEMS_SKETCH_IMPL_HPP_

#include <cstring>
#include <limits>
#include <sstream>
#include <stdexcept>

#include "memory_operations.hpp"

namespace datasketches {

// clang++ seems to require this declaration for CMAKE_BUILD_TYPE='Debug"
template<typename T, typename W, typename H, typename E, typename S, typename A>
const uint8_t frequent_items_sketch<T, W, H, E, S, A>::LG_MIN_MAP_SIZE;

template<typename T, typename W, typename H, typename E, typename S, typename A>
frequent_items_sketch<T, W, H, E, S, A>::frequent_items_sketch(uint8_t lg_max_map_size, uint8_t lg_start_map_size, const A& allocator):
total_weight(0),
offset(0),
map(
  std::max(lg_start_map_size, frequent_items_sketch::LG_MIN_MAP_SIZE),
  std::max(lg_max_map_size, frequent_items_sketch::LG_MIN_MAP_SIZE),
  allocator
)
{
  if (lg_start_map_size > lg_max_map_size) throw std::invalid_argument("starting size must not be greater than maximum size");
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
void frequent_items_sketch<T, W, H, E, S, A>::update(const T& item, W weight) {
  check_weight(weight);
  if (weight == 0) return;
  total_weight += weight;
  offset += map.adjust_or_insert(item, weight);
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
void frequent_items_sketch<T, W, H, E, S, A>::update(T&& item, W weight) {
  check_weight(weight);
  if (weight == 0) return;
  total_weight += weight;
  offset += map.adjust_or_insert(std::move(item), weight);
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
void frequent_items_sketch<T, W, H, E, S, A>::merge(const frequent_items_sketch& other) {
  if (other.is_empty()) return;
  const W merged_total_weight = total_weight + other.get_total_weight(); // for correction at the end
  for (auto it: other.map) {
    update(it.first, it.second);
  }
  offset += other.offset;
  total_weight = merged_total_weight;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
void frequent_items_sketch<T, W, H, E, S, A>::merge(frequent_items_sketch&& other) {
  if (other.is_empty()) return;
  const W merged_total_weight = total_weight + other.get_total_weight(); // for correction at the end
  for (auto it: other.map) {
    update(std::move(it.first), it.second);
  }
  offset += other.offset;
  total_weight = merged_total_weight;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
bool frequent_items_sketch<T, W, H, E, S, A>::is_empty() const {
  return map.get_num_active() == 0;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
uint32_t frequent_items_sketch<T, W, H, E, S, A>::get_num_active_items() const {
  return map.get_num_active();
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
W frequent_items_sketch<T, W, H, E, S, A>::get_total_weight() const {
  return total_weight;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
W frequent_items_sketch<T, W, H, E, S, A>::get_estimate(const T& item) const {
  // if item is tracked estimate = weight + offset, otherwise 0
  const W weight = map.get(item);
  if (weight > 0) return weight + offset;
  return 0;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
W frequent_items_sketch<T, W, H, E, S, A>::get_lower_bound(const T& item) const {
  return map.get(item);
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
W frequent_items_sketch<T, W, H, E, S, A>::get_upper_bound(const T& item) const {
  return map.get(item) + offset;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
W frequent_items_sketch<T, W, H, E, S, A>::get_maximum_error() const {
  return offset;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
double frequent_items_sketch<T, W, H, E, S, A>::get_epsilon() const {
  return EPSILON_FACTOR / (1 << map.get_lg_max_size());
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
double frequent_items_sketch<T, W, H, E, S, A>::get_epsilon(uint8_t lg_max_map_size) {
  return EPSILON_FACTOR / (1 << lg_max_map_size);
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
double frequent_items_sketch<T, W, H, E, S, A>::get_apriori_error(uint8_t lg_max_map_size, W estimated_total_weight) {
  return get_epsilon(lg_max_map_size) * estimated_total_weight;
}


template<typename T, typename W, typename H, typename E, typename S, typename A>
typename frequent_items_sketch<T, W, H, E, S, A>::vector_row
frequent_items_sketch<T, W, H, E, S, A>::get_frequent_items(frequent_items_error_type err_type) const {
  return get_frequent_items(err_type, get_maximum_error());
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
typename frequent_items_sketch<T, W, H, E, S, A>::vector_row
frequent_items_sketch<T, W, H, E, S, A>::get_frequent_items(frequent_items_error_type err_type, W threshold) const {
  vector_row items(map.get_allocator());
  for (auto it: map) {
    const W lb = it.second;
    const W ub = it.second + offset;
    if ((err_type == NO_FALSE_NEGATIVES && ub > threshold) || (err_type == NO_FALSE_POSITIVES && lb > threshold)) {
      items.push_back(row(&it.first, it.second, offset));
    }
  }
  // sort by estimate in descending order
  std::sort(items.begin(), items.end(), [](row a, row b){ return a.get_estimate() > b.get_estimate(); });
  return items;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
template<typename SerDe>
void frequent_items_sketch<T, W, H, E, S, A>::serialize(std::ostream& os, const SerDe& sd) const {
  const uint8_t preamble_longs = is_empty() ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_NONEMPTY;
  write(os, preamble_longs);
  const uint8_t serial_version = SERIAL_VERSION;
  write(os, serial_version);
  const uint8_t family = FAMILY_ID;
  write(os, family);
  const uint8_t lg_max_size = map.get_lg_max_size();
  write(os, lg_max_size);
  const uint8_t lg_cur_size = map.get_lg_cur_size();
  write(os, lg_cur_size);
  const uint8_t flags_byte(
    (is_empty() ? 1 << flags::IS_EMPTY : 0)
  );
  write(os, flags_byte);
  const uint16_t unused16 = 0;
  write(os, unused16);
  if (!is_empty()) {
    const uint32_t num_items = map.get_num_active();
    write(os, num_items);
    const uint32_t unused32 = 0;
    write(os, unused32);
    write(os, total_weight);
    write(os, offset);

    // copy active items and their weights to use batch serialization
    using AllocW = typename std::allocator_traits<A>::template rebind_alloc<W>;
    AllocW aw(map.get_allocator());
    W* weights = aw.allocate(num_items);
    A alloc(map.get_allocator());
    T* items = alloc.allocate(num_items);
    uint32_t i = 0;
    for (auto it: map) {
      new (&items[i]) T(it.first);
      weights[i++] = it.second;
    }
    write(os, weights, sizeof(W) * num_items);
    aw.deallocate(weights, num_items);
    sd.serialize(os, items, num_items);
    for (i = 0; i < num_items; i++) items[i].~T();
    alloc.deallocate(items, num_items);
  }
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
template<typename SerDe>
size_t frequent_items_sketch<T, W, H, E, S, A>::get_serialized_size_bytes(const SerDe& sd) const {
  if (is_empty()) return PREAMBLE_LONGS_EMPTY * sizeof(uint64_t);
  size_t size = PREAMBLE_LONGS_NONEMPTY * sizeof(uint64_t) + map.get_num_active() * sizeof(W);
  for (auto it: map) size += sd.size_of_item(it.first);
  return size;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
template<typename SerDe>
auto frequent_items_sketch<T, W, H, E, S, A>::serialize(unsigned header_size_bytes, const SerDe& sd) const -> vector_bytes {
  const size_t size = header_size_bytes + get_serialized_size_bytes(sd);
  vector_bytes bytes(size, 0, map.get_allocator());
  uint8_t* ptr = bytes.data() + header_size_bytes;
  uint8_t* end_ptr = ptr + size;

  const uint8_t preamble_longs = is_empty() ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_NONEMPTY;
  ptr += copy_to_mem(preamble_longs, ptr);
  const uint8_t serial_version = SERIAL_VERSION;
  ptr += copy_to_mem(serial_version, ptr);
  const uint8_t family = FAMILY_ID;
  ptr += copy_to_mem(family, ptr);
  const uint8_t lg_max_size = map.get_lg_max_size();
  ptr += copy_to_mem(lg_max_size, ptr);
  const uint8_t lg_cur_size = map.get_lg_cur_size();
  ptr += copy_to_mem(lg_cur_size, ptr);
  const uint8_t flags_byte(
    (is_empty() ? 1 << flags::IS_EMPTY : 0)
  );
  ptr += copy_to_mem(flags_byte, ptr);
  ptr += sizeof(uint16_t); // unused
  if (!is_empty()) {
    const uint32_t num_items = map.get_num_active();
    ptr += copy_to_mem(num_items, ptr);
    ptr += sizeof(uint32_t); // unused
    ptr += copy_to_mem(total_weight, ptr);
    ptr += copy_to_mem(offset, ptr);

    // copy active items and their weights to use batch serialization
    using AllocW = typename std::allocator_traits<A>::template rebind_alloc<W>;
    AllocW aw(map.get_allocator());
    W* weights = aw.allocate(num_items);
    A alloc(map.get_allocator());
    T* items = alloc.allocate(num_items);
    uint32_t i = 0;
    for (auto it: map) {
      new (&items[i]) T(it.first);
      weights[i++] = it.second;
    }
    ptr += copy_to_mem(weights, ptr, sizeof(W) * num_items);
    aw.deallocate(weights, num_items);
    const size_t bytes_remaining = end_ptr - ptr;
    ptr += sd.serialize(ptr, bytes_remaining, items, num_items);
    for (i = 0; i < num_items; i++) items[i].~T();
    alloc.deallocate(items, num_items);
  }
  return bytes;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
class frequent_items_sketch<T, W, H, E, S, A>::items_deleter {
public:
  items_deleter(uint32_t num, bool destroy, const A& allocator):
    allocator_(allocator), num_(num), destroy_(destroy) {}
  void set_destroy(bool destroy) { destroy_ = destroy; }
  void operator() (T* ptr) {
    if (ptr != nullptr) {
      if (destroy_) {
        for (uint32_t i = 0; i < num_; ++i) ptr[i].~T();
      }
      allocator_.deallocate(ptr, num_);
    }
  }
private:
  A allocator_;
  uint32_t num_;
  bool destroy_;
};

template<typename T, typename W, typename H, typename E, typename S, typename A>
frequent_items_sketch<T, W, H, E, S, A> frequent_items_sketch<T, W, H, E, S, A>::deserialize(std::istream& is, const A& allocator) {
  return deserialize(is, S(), allocator);
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
template<typename SerDe>
frequent_items_sketch<T, W, H, E, S, A> frequent_items_sketch<T, W, H, E, S, A>::deserialize(std::istream& is, const SerDe& sd, const A& allocator) {
  const auto preamble_longs = read<uint8_t>(is);
  const auto serial_version = read<uint8_t>(is);
  const auto family_id = read<uint8_t>(is);
  const auto lg_max_size = read<uint8_t>(is);
  const auto lg_cur_size = read<uint8_t>(is);
  const auto flags_byte = read<uint8_t>(is);
  read<uint16_t>(is); // unused

  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);

  check_preamble_longs(preamble_longs, is_empty);
  check_serial_version(serial_version);
  check_family_id(family_id);
  check_size(lg_cur_size, lg_max_size);

  frequent_items_sketch<T, W, H, E, S, A> sketch(lg_max_size, lg_cur_size, allocator);
  if (!is_empty) {
    const auto num_items = read<uint32_t>(is);
    read<uint32_t>(is); // unused
    const auto total_weight = read<W>(is);
    const auto offset = read<W>(is);

    // batch deserialization with intermediate array of items and weights
    using AllocW = typename std::allocator_traits<A>::template rebind_alloc<W>;
    std::vector<W, AllocW> weights(num_items, 0, allocator);
    read(is, weights.data(), sizeof(W) * num_items);
    A alloc(allocator);
    std::unique_ptr<T, items_deleter> items(alloc.allocate(num_items), items_deleter(num_items, false, alloc));
    sd.deserialize(is, items.get(), num_items);
    items.get_deleter().set_destroy(true); // serde did not throw, so the items must be constructed
    for (uint32_t i = 0; i < num_items; i++) {
      sketch.update(std::move(items.get()[i]), weights[i]);
    }
    sketch.total_weight = total_weight;
    sketch.offset = offset;
  }
  if (!is.good())
    throw std::runtime_error("error reading from std::istream"); 
  return sketch;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
frequent_items_sketch<T, W, H, E, S, A> frequent_items_sketch<T, W, H, E, S, A>::deserialize(const void* bytes, size_t size, const A& allocator) {
  return deserialize(bytes, size, S(), allocator);
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
template<typename SerDe>
frequent_items_sketch<T, W, H, E, S, A> frequent_items_sketch<T, W, H, E, S, A>::deserialize(const void* bytes, size_t size, const SerDe& sd, const A& allocator) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  const char* base = static_cast<const char*>(bytes);
  uint8_t preamble_longs;
  ptr += copy_from_mem(ptr, preamble_longs);
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, serial_version);
  uint8_t family_id;
  ptr += copy_from_mem(ptr, family_id);
  uint8_t lg_max_size;
  ptr += copy_from_mem(ptr, lg_max_size);
  uint8_t lg_cur_size;
  ptr += copy_from_mem(ptr, lg_cur_size);
  uint8_t flags_byte;
  ptr += copy_from_mem(ptr, flags_byte);
  ptr += sizeof(uint16_t); // unused

  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);

  check_preamble_longs(preamble_longs, is_empty);
  check_serial_version(serial_version);
  check_family_id(family_id);
  check_size(lg_cur_size, lg_max_size);
  ensure_minimum_memory(size, preamble_longs * sizeof(uint64_t));

  frequent_items_sketch<T, W, H, E, S, A> sketch(lg_max_size, lg_cur_size, allocator);
  if (!is_empty) {
    uint32_t num_items;
    ptr += copy_from_mem(ptr, num_items);
    ptr += sizeof(uint32_t); // unused
    W total_weight;
    ptr += copy_from_mem(ptr, total_weight);
    W offset;
    ptr += copy_from_mem(ptr, offset);

    ensure_minimum_memory(size, ptr - base + (sizeof(W) * num_items));
    // batch deserialization with intermediate array of items and weights
    using AllocW = typename std::allocator_traits<A>::template rebind_alloc<W>;
    std::vector<W, AllocW> weights(num_items, 0, allocator);
    ptr += copy_from_mem(ptr, weights.data(), sizeof(W) * num_items);
    A alloc(allocator);
    std::unique_ptr<T, items_deleter> items(alloc.allocate(num_items), items_deleter(num_items, false, alloc));
    const size_t bytes_remaining = size - (ptr - base);
    ptr += sd.deserialize(ptr, bytes_remaining, items.get(), num_items);
    items.get_deleter().set_destroy(true); // serde did not throw, so the items must be constructed
    for (uint32_t i = 0; i < num_items; i++) {
      sketch.update(std::move(items.get()[i]), weights[i]);
    }

    sketch.total_weight = total_weight;
    sketch.offset = offset;
  }
  return sketch;
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
void frequent_items_sketch<T, W, H, E, S, A>::check_preamble_longs(uint8_t preamble_longs, bool is_empty) {
  if (is_empty) {
    if (preamble_longs != PREAMBLE_LONGS_EMPTY) {
      throw std::invalid_argument("Possible corruption: preamble longs of an empty sketch must be " + std::to_string(PREAMBLE_LONGS_EMPTY) + ": " + std::to_string(preamble_longs));
    }
  } else {
    if (preamble_longs != PREAMBLE_LONGS_NONEMPTY) {
      throw std::invalid_argument("Possible corruption: preamble longs of an non-empty sketch must be " + std::to_string(PREAMBLE_LONGS_NONEMPTY) + ": " + std::to_string(preamble_longs));
    }
  }
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
void frequent_items_sketch<T, W, H, E, S, A>::check_serial_version(uint8_t serial_version) {
  if (serial_version != SERIAL_VERSION) {
    throw std::invalid_argument("Possible corruption: serial version must be " + std::to_string(SERIAL_VERSION) + ": " + std::to_string(serial_version));
  }
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
void frequent_items_sketch<T, W, H, E, S, A>::check_family_id(uint8_t family_id) {
  if (family_id != FAMILY_ID) {
    throw std::invalid_argument("Possible corruption: family ID must be " + std::to_string(FAMILY_ID) + ": " + std::to_string(family_id));
  }
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
void frequent_items_sketch<T, W, H, E, S, A>::check_size(uint8_t lg_cur_size, uint8_t lg_max_size) {
  if (lg_cur_size > lg_max_size) {
    throw std::invalid_argument("Possible corruption: expected lg_cur_size <= lg_max_size: " + std::to_string(lg_cur_size) + " <= " + std::to_string(lg_max_size));
  }
  if (lg_cur_size < LG_MIN_MAP_SIZE) {
    throw std::invalid_argument("Possible corruption: lg_cur_size must not be less than " + std::to_string(LG_MIN_MAP_SIZE) + ": " + std::to_string(lg_cur_size));
  }
}

template<typename T, typename W, typename H, typename E, typename S, typename A>
string<A> frequent_items_sketch<T, W, H, E, S, A>::to_string(bool print_items) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### Frequent items sketch summary:" << std::endl;
  os << "   lg cur map size  : " << (int) map.get_lg_cur_size() << std::endl;
  os << "   lg max map size  : " << (int) map.get_lg_max_size() << std::endl;
  os << "   num active items : " << get_num_active_items() << std::endl;
  os << "   total weight     : " << get_total_weight() << std::endl;
  os << "   max error        : " << get_maximum_error() << std::endl;
  os << "### End sketch summary" << std::endl;
  if (print_items) {
    vector_row items;
    for (auto it: map) {
      items.push_back(row(&it.first, it.second, offset));
    }
    // sort by estimate in descending order
    std::sort(items.begin(), items.end(), [](row a, row b){ return a.get_estimate() > b.get_estimate(); });
    os << "### Items in descending order by estimate" << std::endl;
    os << "   item, estimate, lower bound, upper bound" << std::endl;
    for (auto it: items) {
      os << "   " << it.get_item() << ", " << it.get_estimate() << ", "
         << it.get_lower_bound() << ", " << it.get_upper_bound() << std::endl;
    }
    os << "### End items" << std::endl;
  }
  return string<A>(os.str().c_str(), map.get_allocator());
}

// version for integral signed type
template<typename T, typename W, typename H, typename E, typename S, typename A>
template<typename WW, typename std::enable_if<std::is_integral<WW>::value && std::is_signed<WW>::value, int>::type>
void frequent_items_sketch<T, W, H, E, S, A>::check_weight(WW weight) {
  if (weight < 0) {
    throw std::invalid_argument("weight must be non-negative");
  }
}

// version for integral unsigned type - no-op
template<typename T, typename W, typename H, typename E, typename S, typename A>
template<typename WW, typename std::enable_if<std::is_integral<WW>::value && std::is_unsigned<WW>::value, int>::type>
void frequent_items_sketch<T, W, H, E, S, A>::check_weight(WW) {}

// version for floating point type
template<typename T, typename W, typename H, typename E, typename S, typename A>
template<typename WW, typename std::enable_if<std::is_floating_point<WW>::value, int>::type>
void frequent_items_sketch<T, W, H, E, S, A>::check_weight(WW weight) {
  if (weight < 0) {
    throw std::invalid_argument("weight must be non-negative");
  }
  if (std::isnan(weight)) {
    throw std::invalid_argument("weight must be a valid number");
  }
  if (std::isinf(weight)) {
    throw std::invalid_argument("weight must be finite");
  }
}

}

#endif
