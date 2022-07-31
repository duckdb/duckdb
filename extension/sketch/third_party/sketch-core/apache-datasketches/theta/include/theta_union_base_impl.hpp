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

#ifndef THETA_UNION_BASE_IMPL_HPP_
#define THETA_UNION_BASE_IMPL_HPP_

#include <algorithm>
#include <stdexcept>

#include "conditional_forward.hpp"

namespace datasketches {

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
theta_union_base<EN, EK, P, S, CS, A>::theta_union_base(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf,
    float p, uint64_t theta, uint64_t seed, const P& policy, const A& allocator):
policy_(policy),
table_(lg_cur_size, lg_nom_size, rf, p, theta, seed, allocator),
union_theta_(table_.theta_)
{}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
template<typename SS>
void theta_union_base<EN, EK, P, S, CS, A>::update(SS&& sketch) {
  if (sketch.is_empty()) return;
  if (sketch.get_seed_hash() != compute_seed_hash(table_.seed_)) throw std::invalid_argument("seed hash mismatch");
  table_.is_empty_ = false;
  if (sketch.get_theta64() < union_theta_) union_theta_ = sketch.get_theta64();
  for (auto& entry: sketch) {
    const uint64_t hash = EK()(entry);
    if (hash < union_theta_ && hash < table_.theta_) {
      auto result = table_.find(hash);
      if (!result.second) {
        table_.insert(result.first, conditional_forward<SS>(entry));
      } else {
        policy_(*result.first, conditional_forward<SS>(entry));
      }
    } else {
      if (sketch.is_ordered()) break; // early stop
    }
  }
  if (table_.theta_ < union_theta_) union_theta_ = table_.theta_;
}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
CS theta_union_base<EN, EK, P, S, CS, A>::get_result(bool ordered) const {
  std::vector<EN, A> entries(table_.allocator_);
  if (table_.is_empty_) return CS(true, true, compute_seed_hash(table_.seed_), union_theta_, std::move(entries));
  entries.reserve(table_.num_entries_);
  uint64_t theta = std::min(union_theta_, table_.theta_);
  const uint32_t nominal_num = 1 << table_.lg_nom_size_;
  if (union_theta_ >= theta && table_.num_entries_ <= nominal_num) {
    std::copy_if(table_.begin(), table_.end(), std::back_inserter(entries), key_not_zero<EN, EK>());
  } else {
    std::copy_if(table_.begin(), table_.end(), std::back_inserter(entries), key_not_zero_less_than<uint64_t, EN, EK>(theta));
    if (entries.size() > nominal_num) {
      std::nth_element(entries.begin(), entries.begin() + nominal_num, entries.end(), comparator());
      theta = EK()(entries[nominal_num]);
      entries.erase(entries.begin() + nominal_num, entries.end());
      entries.shrink_to_fit();
    }
  }
  if (ordered) std::sort(entries.begin(), entries.end(), comparator());
  return CS(table_.is_empty_, ordered, compute_seed_hash(table_.seed_), theta, std::move(entries));
}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
const P& theta_union_base<EN, EK, P, S, CS, A>::get_policy() const {
  return policy_;
}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
void theta_union_base<EN, EK, P, S, CS, A>::reset() {
  table_.reset();
  union_theta_ = table_.theta_;
}

} /* namespace datasketches */

#endif
