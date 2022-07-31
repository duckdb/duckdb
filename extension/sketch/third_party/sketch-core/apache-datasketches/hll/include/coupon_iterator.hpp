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

#ifndef _INTARRAYPAIRITERATOR_HPP_
#define _INTARRAYPAIRITERATOR_HPP_

namespace datasketches {

template<typename A>
class coupon_iterator: public std::iterator<std::input_iterator_tag, uint32_t> {
public:
  coupon_iterator(const uint32_t* array, size_t array_slze, size_t index, bool all);
  coupon_iterator& operator++();
  bool operator!=(const coupon_iterator& other) const;
  uint32_t operator*() const;
private:
  const uint32_t* array_;
  size_t array_size_;
  size_t index_;
  bool all_;
};

}

#include "coupon_iterator-internal.hpp"

#endif /* _INTARRAYPAIRITERATOR_HPP_ */
