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

namespace datasketches {

template<typename A>
update_array_of_doubles_sketch_alloc<A>::update_array_of_doubles_sketch_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf,
    float p, uint64_t theta, uint64_t seed, const array_of_doubles_update_policy<A>& policy, const A& allocator):
Base(lg_cur_size, lg_nom_size, rf, p, theta, seed, policy, allocator) {}


template<typename A>
uint8_t update_array_of_doubles_sketch_alloc<A>::get_num_values() const {
  return this->policy_.get_num_values();
}

template<typename A>
compact_array_of_doubles_sketch_alloc<A> update_array_of_doubles_sketch_alloc<A>::compact(bool ordered) const {
  return compact_array_of_doubles_sketch_alloc<A>(*this, ordered);
}

// builder

template<typename A>
update_array_of_doubles_sketch_alloc<A>::builder::builder(const array_of_doubles_update_policy<A>& policy, const A& allocator):
tuple_base_builder<builder, array_of_doubles_update_policy<A>, A>(policy, allocator) {}

template<typename A>
update_array_of_doubles_sketch_alloc<A> update_array_of_doubles_sketch_alloc<A>::builder::build() const {
  return update_array_of_doubles_sketch_alloc<A>(this->starting_lg_size(), this->lg_k_, this->rf_, this->p_, this->starting_theta(), this->seed_, this->policy_, this->allocator_);
}

// compact sketch

template<typename A>
template<typename S>
compact_array_of_doubles_sketch_alloc<A>::compact_array_of_doubles_sketch_alloc(const S& other, bool ordered):
Base(other, ordered), num_values_(other.get_num_values()) {}

template<typename A>
compact_array_of_doubles_sketch_alloc<A>::compact_array_of_doubles_sketch_alloc(bool is_empty, bool is_ordered,
    uint16_t seed_hash, uint64_t theta, std::vector<Entry, AllocEntry>&& entries, uint8_t num_values):
Base(is_empty, is_ordered, seed_hash, theta, std::move(entries)), num_values_(num_values) {}

template<typename A>
compact_array_of_doubles_sketch_alloc<A>::compact_array_of_doubles_sketch_alloc(uint8_t num_values, Base&& base):
Base(std::move(base)), num_values_(num_values) {}

template<typename A>
uint8_t compact_array_of_doubles_sketch_alloc<A>::get_num_values() const {
  return num_values_;
}

template<typename A>
void compact_array_of_doubles_sketch_alloc<A>::serialize(std::ostream& os) const {
  const uint8_t preamble_longs = 1;
  write(os, preamble_longs);
  const uint8_t serial_version = SERIAL_VERSION;
  write(os, serial_version);
  const uint8_t family = SKETCH_FAMILY;
  write(os, family);
  const uint8_t type = SKETCH_TYPE;
  write(os, type);
  const uint8_t flags_byte(
    (this->is_empty() ? 1 << flags::IS_EMPTY : 0) |
    (this->get_num_retained() > 0 ? 1 << flags::HAS_ENTRIES : 0) |
    (this->is_ordered() ? 1 << flags::IS_ORDERED : 0)
  );
  write(os, flags_byte);
  write(os, num_values_);
  const uint16_t seed_hash = this->get_seed_hash();
  write(os, seed_hash);
  write(os, this->theta_);
  if (this->get_num_retained() > 0) {
    const uint32_t num_entries = static_cast<uint32_t>(this->entries_.size());
    write(os, num_entries);
    const uint32_t unused32 = 0;
    write(os, unused32);
    for (const auto& it: this->entries_) {
      write(os, it.first);
    }
    for (const auto& it: this->entries_) {
      write(os, it.second.data(), it.second.size() * sizeof(double));
    }
  }
}

template<typename A>
auto compact_array_of_doubles_sketch_alloc<A>::serialize(unsigned header_size_bytes) const -> vector_bytes {
  const uint8_t preamble_longs = 1;
  const size_t size = header_size_bytes + 16 // preamble and theta
      + (this->entries_.size() > 0 ? 8 : 0)
      + (sizeof(uint64_t) + sizeof(double) * num_values_) * this->entries_.size();
  vector_bytes bytes(size, 0, this->entries_.get_allocator());
  uint8_t* ptr = bytes.data() + header_size_bytes;

  ptr += copy_to_mem(preamble_longs, ptr);
  const uint8_t serial_version = SERIAL_VERSION;
  ptr += copy_to_mem(serial_version, ptr);
  const uint8_t family = SKETCH_FAMILY;
  ptr += copy_to_mem(family, ptr);
  const uint8_t type = SKETCH_TYPE;
  ptr += copy_to_mem(type, ptr);
  const uint8_t flags_byte(
    (this->is_empty() ? 1 << flags::IS_EMPTY : 0) |
    (this->get_num_retained() ? 1 << flags::HAS_ENTRIES : 0) |
    (this->is_ordered() ? 1 << flags::IS_ORDERED : 0)
  );
  ptr += copy_to_mem(flags_byte, ptr);
  ptr += copy_to_mem(num_values_, ptr);
  const uint16_t seed_hash = this->get_seed_hash();
  ptr += copy_to_mem(seed_hash, ptr);
  ptr += copy_to_mem((this->theta_), ptr);
  if (this->get_num_retained() > 0) {
    const uint32_t num_entries = static_cast<uint32_t>(this->entries_.size());
    ptr += copy_to_mem(num_entries, ptr);
    ptr += sizeof(uint32_t); // unused
    for (const auto& it: this->entries_) {
      ptr += copy_to_mem(it.first, ptr);
    }
    for (const auto& it: this->entries_) {
      ptr += copy_to_mem(it.second.data(), ptr, it.second.size() * sizeof(double));
    }
  }
  return bytes;
}

template<typename A>
compact_array_of_doubles_sketch_alloc<A> compact_array_of_doubles_sketch_alloc<A>::deserialize(std::istream& is, uint64_t seed, const A& allocator) {
  read<uint8_t>(is); // unused
  const auto serial_version = read<uint8_t>(is);
  const auto family = read<uint8_t>(is);
  const auto type = read<uint8_t>(is);
  const auto flags_byte = read<uint8_t>(is);
  const auto num_values = read<uint8_t>(is);
  const auto seed_hash = read<uint16_t>(is);
  checker<true>::check_serial_version(serial_version, SERIAL_VERSION);
  checker<true>::check_sketch_family(family, SKETCH_FAMILY);
  checker<true>::check_sketch_type(type, SKETCH_TYPE);
  const bool has_entries = flags_byte & (1 << flags::HAS_ENTRIES);
  if (has_entries) checker<true>::check_seed_hash(seed_hash, compute_seed_hash(seed));

  const auto theta = read<uint64_t>(is);
  std::vector<Entry, AllocEntry> entries(allocator);
  if (has_entries) {
    const auto num_entries = read<uint32_t>(is);
    read<uint32_t>(is); // unused
    entries.reserve(num_entries);
    std::vector<uint64_t, AllocU64> keys(num_entries, 0, allocator);
    read(is, keys.data(), num_entries * sizeof(uint64_t));
    for (size_t i = 0; i < num_entries; ++i) {
      aod<A> summary(num_values, allocator);
      read(is, summary.data(), num_values * sizeof(double));
      entries.push_back(Entry(keys[i], std::move(summary)));
    }
  }
  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);
  const bool is_ordered = flags_byte & (1 << flags::IS_ORDERED);
  return compact_array_of_doubles_sketch_alloc(is_empty, is_ordered, seed_hash, theta, std::move(entries), num_values);
}

template<typename A>
compact_array_of_doubles_sketch_alloc<A> compact_array_of_doubles_sketch_alloc<A>::deserialize(const void* bytes, size_t size, uint64_t seed, const A& allocator) {
  ensure_minimum_memory(size, 16);
  const char* ptr = static_cast<const char*>(bytes);
  ptr += sizeof(uint8_t); // unused
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, serial_version);
  uint8_t family;
  ptr += copy_from_mem(ptr, family);
  uint8_t type;
  ptr += copy_from_mem(ptr, type);
  uint8_t flags_byte;
  ptr += copy_from_mem(ptr, flags_byte);
  uint8_t num_values;
  ptr += copy_from_mem(ptr, num_values);
  uint16_t seed_hash;
  ptr += copy_from_mem(ptr, seed_hash);
  checker<true>::check_serial_version(serial_version, SERIAL_VERSION);
  checker<true>::check_sketch_family(family, SKETCH_FAMILY);
  checker<true>::check_sketch_type(type, SKETCH_TYPE);
  const bool has_entries = flags_byte & (1 << flags::HAS_ENTRIES);
  if (has_entries) checker<true>::check_seed_hash(seed_hash, compute_seed_hash(seed));

  uint64_t theta;
  ptr += copy_from_mem(ptr, theta);
  std::vector<Entry, AllocEntry> entries(allocator);
  if (has_entries) {
    ensure_minimum_memory(size, 24);
    uint32_t num_entries;
    ptr += copy_from_mem(ptr, num_entries);
    ptr += sizeof(uint32_t); // unused
    ensure_minimum_memory(size, 24 + (sizeof(uint64_t) + sizeof(double) * num_values) * num_entries);
    entries.reserve(num_entries);
    std::vector<uint64_t, AllocU64> keys(num_entries, 0, allocator);
    ptr += copy_from_mem(ptr, keys.data(), sizeof(uint64_t) * num_entries);
    for (size_t i = 0; i < num_entries; ++i) {
      aod<A> summary(num_values, allocator);
      ptr += copy_from_mem(ptr, summary.data(), num_values * sizeof(double));
      entries.push_back(Entry(keys[i], std::move(summary)));
    }
  }
  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);
  const bool is_ordered = flags_byte & (1 << flags::IS_ORDERED);
  return compact_array_of_doubles_sketch_alloc(is_empty, is_ordered, seed_hash, theta, std::move(entries), num_values);
}

} /* namespace datasketches */
