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

#ifndef QUANTILE_SKETCH_SORTED_VIEW_IMPL_HPP_
#define QUANTILE_SKETCH_SORTED_VIEW_IMPL_HPP_

#include <algorithm>
#include <stdexcept>

namespace datasketches {

template<typename T, typename C, typename A>
quantile_sketch_sorted_view<T, C, A>::quantile_sketch_sorted_view(uint32_t num, const A& allocator):
total_weight_(0),
entries_(allocator)
{
  entries_.reserve(num);
}

template<typename T, typename C, typename A>
template<typename Iterator>
void quantile_sketch_sorted_view<T, C, A>::add(Iterator first, Iterator last, uint64_t weight) {
  const size_t size_before = entries_.size();
  for (auto it = first; it != last; ++it) entries_.push_back(Entry(ref_helper(*it), weight));
  if (size_before > 0) {
    Container tmp(entries_.get_allocator());
    tmp.reserve(entries_.capacity());
    std::merge(
        entries_.begin(), entries_.begin() + size_before,
        entries_.begin() + size_before, entries_.end(),
        std::back_inserter(tmp), compare_pairs_by_first()
    );
    std::swap(tmp, entries_);
  }
}

template<typename T, typename C, typename A>
template<bool inclusive>
void quantile_sketch_sorted_view<T, C, A>::convert_to_cummulative() {
  uint64_t subtotal = 0;
  for (auto& entry: entries_) {
    const uint64_t new_subtotal = subtotal + entry.second;
    entry.second = inclusive ? new_subtotal : subtotal;
    subtotal = new_subtotal;
  }
  total_weight_ = subtotal;
}

template<typename T, typename C, typename A>
auto quantile_sketch_sorted_view<T, C, A>::get_quantile(double rank) const -> quantile_return_type {
  if (total_weight_ == 0) throw std::invalid_argument("supported for cumulative weight only");
  uint64_t weight = static_cast<uint64_t>(rank * total_weight_);
  auto it = std::lower_bound(entries_.begin(), entries_.end(), make_dummy_entry<T>(weight), compare_pairs_by_second());
  if (it == entries_.end()) return deref_helper(entries_[entries_.size() - 1].first);
  return deref_helper(it->first);
}

template<typename T, typename C, typename A>
auto quantile_sketch_sorted_view<T, C, A>::begin() const -> const_iterator {
  return entries_.begin();
}

template<typename T, typename C, typename A>
auto quantile_sketch_sorted_view<T, C, A>::end() const -> const_iterator {
  return entries_.end();
}

template<typename T, typename C, typename A>
size_t quantile_sketch_sorted_view<T, C, A>::size() const {
  return entries_.size();
}

} /* namespace datasketches */

#endif
