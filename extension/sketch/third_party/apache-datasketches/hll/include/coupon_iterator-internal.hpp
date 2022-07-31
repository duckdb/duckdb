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

#ifndef _INTARRAYPAIRITERATOR_INTERNAL_HPP_
#define _INTARRAYPAIRITERATOR_INTERNAL_HPP_

#include "HllUtil.hpp"

namespace datasketches {

template<typename A>
coupon_iterator<A>::coupon_iterator(const uint32_t* array, size_t array_size, size_t index, bool all):
array_(array), array_size_(array_size), index_(index), all_(all) {
  while (index_ < array_size_) {
    if (all_ || array_[index_] != hll_constants::EMPTY) break;
    ++index_;
  }
}

template<typename A>
coupon_iterator<A>& coupon_iterator<A>::operator++() {
  while (++index_ < array_size_) {
    if (all_ || array_[index_] != hll_constants::EMPTY) break;
  }
  return *this;
}

template<typename A>
bool coupon_iterator<A>::operator!=(const coupon_iterator& other) const {
  return index_ != other.index_;
}

template<typename A>
uint32_t coupon_iterator<A>::operator*() const {
  return array_[index_];
}

}

#endif // _INTARRAYPAIRITERATOR_INTERNAL_HPP_
