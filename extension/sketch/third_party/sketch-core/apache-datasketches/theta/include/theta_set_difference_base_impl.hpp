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

#ifndef THETA_A_SET_DIFFERENCE_BASE_IMPL_HPP_
#define THETA_A_SET_DIFFERENCE_BASE_IMPL_HPP_

#include <algorithm>
#include <stdexcept>

#include "conditional_back_inserter.hpp"
#include "conditional_forward.hpp"

namespace datasketches {

template<typename EN, typename EK, typename CS, typename A>
theta_set_difference_base<EN, EK, CS, A>::theta_set_difference_base(uint64_t seed, const A& allocator):
allocator_(allocator),
seed_hash_(compute_seed_hash(seed))
{}

template<typename EN, typename EK, typename CS, typename A>
template<typename FwdSketch, typename Sketch>
CS theta_set_difference_base<EN, EK, CS, A>::compute(FwdSketch&& a, const Sketch& b, bool ordered) const {
  if (a.is_empty() || (a.get_num_retained() > 0 && b.is_empty())) return CS(a, ordered);
  if (a.get_seed_hash() != seed_hash_) throw std::invalid_argument("A seed hash mismatch");
  if (b.get_seed_hash() != seed_hash_) throw std::invalid_argument("B seed hash mismatch");

  const uint64_t theta = std::min(a.get_theta64(), b.get_theta64());
  std::vector<EN, A> entries(allocator_);
  bool is_empty = a.is_empty();

  if (b.get_num_retained() == 0) {
    std::copy_if(forward_begin(std::forward<FwdSketch>(a)), forward_end(std::forward<FwdSketch>(a)), std::back_inserter(entries),
        key_less_than<uint64_t, EN, EK>(theta));
  } else {
    if (a.is_ordered() && b.is_ordered()) { // sort-based
      std::set_difference(forward_begin(std::forward<FwdSketch>(a)), forward_end(std::forward<FwdSketch>(a)), b.begin(), b.end(),
          conditional_back_inserter(entries, key_less_than<uint64_t, EN, EK>(theta)), comparator());
    } else { // hash-based
      const uint8_t lg_size = lg_size_from_count(b.get_num_retained(), hash_table::REBUILD_THRESHOLD);
      hash_table table(lg_size, lg_size, hash_table::resize_factor::X1, 1, 0, 0, allocator_); // theta and seed are not used here
      for (const auto& entry: b) {
        const uint64_t hash = EK()(entry);
        if (hash < theta) {
          table.insert(table.find(hash).first, hash);
        } else if (b.is_ordered()) {
          break; // early stop
        }
      }

      // scan A lookup B
      for (auto& entry: a) {
        const uint64_t hash = EK()(entry);
        if (hash < theta) {
          auto result = table.find(hash);
          if (!result.second) entries.push_back(conditional_forward<FwdSketch>(entry));
        } else if (a.is_ordered()) {
          break; // early stop
        }
      }
    }
  }
  if (entries.empty() && theta == theta_constants::MAX_THETA) is_empty = true;
  if (ordered && !a.is_ordered()) std::sort(entries.begin(), entries.end(), comparator());
  return CS(is_empty, a.is_ordered() || ordered, seed_hash_, theta, std::move(entries));
}

} /* namespace datasketches */

#endif
