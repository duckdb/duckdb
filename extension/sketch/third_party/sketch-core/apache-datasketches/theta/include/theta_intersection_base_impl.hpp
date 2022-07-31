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

#include <iostream>
#include <sstream>
#include <algorithm>
#include <stdexcept>

#include "conditional_forward.hpp"

namespace datasketches {

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
theta_intersection_base<EN, EK, P, S, CS, A>::theta_intersection_base(uint64_t seed, const P& policy, const A& allocator):
policy_(policy),
is_valid_(false),
table_(0, 0, resize_factor::X1, 1, theta_constants::MAX_THETA, seed, allocator, false)
{}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
template<typename SS>
void theta_intersection_base<EN, EK, P, S, CS, A>::update(SS&& sketch) {
  if (table_.is_empty_) return;
  if (!sketch.is_empty() && sketch.get_seed_hash() != compute_seed_hash(table_.seed_)) throw std::invalid_argument("seed hash mismatch");
  table_.is_empty_ |= sketch.is_empty();
  table_.theta_ = table_.is_empty_ ? theta_constants::MAX_THETA : std::min(table_.theta_, sketch.get_theta64());
  if (is_valid_ && table_.num_entries_ == 0) return;
  if (sketch.get_num_retained() == 0) {
    is_valid_ = true;
    table_ = hash_table(0, 0, resize_factor::X1, 1, table_.theta_, table_.seed_, table_.allocator_, table_.is_empty_);
    return;
  }
  if (!is_valid_) { // first update, copy or move incoming sketch
    is_valid_ = true;
    const uint8_t lg_size = lg_size_from_count(sketch.get_num_retained(), theta_update_sketch_base<EN, EK, A>::REBUILD_THRESHOLD);
    table_ = hash_table(lg_size, lg_size, resize_factor::X1, 1, table_.theta_, table_.seed_, table_.allocator_, table_.is_empty_);
    for (auto& entry: sketch) {
      auto result = table_.find(EK()(entry));
      if (result.second) {
        throw std::invalid_argument("duplicate key, possibly corrupted input sketch");
      }
      table_.insert(result.first, conditional_forward<SS>(entry));
    }
    if (table_.num_entries_ != sketch.get_num_retained()) throw std::invalid_argument("num entries mismatch, possibly corrupted input sketch");
  } else { // intersection
    const uint32_t max_matches = std::min(table_.num_entries_, sketch.get_num_retained());
    std::vector<EN, A> matched_entries(table_.allocator_);
    matched_entries.reserve(max_matches);
    uint32_t match_count = 0;
    uint32_t count = 0;
    for (auto& entry: sketch) {
      if (EK()(entry) < table_.theta_) {
        auto result = table_.find(EK()(entry));
        if (result.second) {
          if (match_count == max_matches) throw std::invalid_argument("max matches exceeded, possibly corrupted input sketch");
          policy_(*result.first, conditional_forward<SS>(entry));
          matched_entries.push_back(std::move(*result.first));
          ++match_count;
        }
      } else if (sketch.is_ordered()) {
        break; // early stop
      }
      ++count;
    }
    if (count > sketch.get_num_retained()) {
      throw std::invalid_argument(" more keys than expected, possibly corrupted input sketch");
    } else if (!sketch.is_ordered() && count < sketch.get_num_retained()) {
      throw std::invalid_argument(" fewer keys than expected, possibly corrupted input sketch");
    }
    if (match_count == 0) {
      table_ = hash_table(0, 0, resize_factor::X1, 1, table_.theta_, table_.seed_, table_.allocator_, table_.is_empty_);
      if (table_.theta_ == theta_constants::MAX_THETA) table_.is_empty_ = true;
    } else {
      const uint8_t lg_size = lg_size_from_count(match_count, theta_update_sketch_base<EN, EK, A>::REBUILD_THRESHOLD);
      table_ = hash_table(lg_size, lg_size, resize_factor::X1, 1, table_.theta_, table_.seed_, table_.allocator_, table_.is_empty_);
      for (uint32_t i = 0; i < match_count; i++) {
        auto result = table_.find(EK()(matched_entries[i]));
        table_.insert(result.first, std::move(matched_entries[i]));
      }
    }
  }
}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
CS theta_intersection_base<EN, EK, P, S, CS, A>::get_result(bool ordered) const {
  if (!is_valid_) throw std::invalid_argument("calling get_result() before calling update() is undefined");
  std::vector<EN, A> entries(table_.allocator_);
  if (table_.num_entries_ > 0) {
    entries.reserve(table_.num_entries_);
    std::copy_if(table_.begin(), table_.end(), std::back_inserter(entries), key_not_zero<EN, EK>());
    if (ordered) std::sort(entries.begin(), entries.end(), comparator());
  }
  return CS(table_.is_empty_, ordered, compute_seed_hash(table_.seed_), table_.theta_, std::move(entries));
}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
bool theta_intersection_base<EN, EK, P, S, CS, A>::has_result() const {
  return is_valid_;
}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
const P& theta_intersection_base<EN, EK, P, S, CS, A>::get_policy() const {
  return policy_;
}

} /* namespace datasketches */
