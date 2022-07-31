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

#ifndef _HLLSKETCHIMPL_INTERNAL_HPP_
#define _HLLSKETCHIMPL_INTERNAL_HPP_

#include "HllSketchImpl.hpp"
#include "HllSketchImplFactory.hpp"

#include <stdexcept>

namespace datasketches {

template<typename A>
HllSketchImpl<A>::HllSketchImpl(uint8_t lgConfigK, target_hll_type tgtHllType,
                                hll_mode mode, bool startFullSize)
  : lgConfigK_(lgConfigK),
    tgtHllType_(tgtHllType),
    mode_(mode),
    startFullSize_(startFullSize)
{
}

template<typename A>
HllSketchImpl<A>::~HllSketchImpl() {
}

template<typename A>
target_hll_type HllSketchImpl<A>::extractTgtHllType(uint8_t modeByte) {
  switch ((modeByte >> 2) & 0x3) {
  case 0:
    return target_hll_type::HLL_4;
  case 1:
    return target_hll_type::HLL_6;
  case 2:
    return target_hll_type::HLL_8;
  default:
    throw std::invalid_argument("Invalid target HLL type");
  }
}

template<typename A>
hll_mode HllSketchImpl<A>::extractCurMode(uint8_t modeByte) {
  switch (modeByte & 0x3) {
  case 0:
    return hll_mode::LIST;
  case 1:
    return hll_mode::SET;
  case 2:
    return hll_mode::HLL;
  default:
    throw std::invalid_argument("Invalid current sketch mode");
  }
}

template<typename A>
uint8_t HllSketchImpl<A>::makeFlagsByte(bool compact) const {
  uint8_t flags = 0;
  flags |= (isEmpty() ? hll_constants::EMPTY_FLAG_MASK : 0);
  flags |= (compact ? hll_constants::COMPACT_FLAG_MASK : 0);
  flags |= (isOutOfOrderFlag() ? hll_constants::OUT_OF_ORDER_FLAG_MASK : 0);
  flags |= (startFullSize_ ? hll_constants::FULL_SIZE_FLAG_MASK : 0);
  return flags;
}

// lo2bits = curMode, next 2 bits = tgtHllType
// Dec  Lo4Bits TgtHllType, CurMode
//   0     0000      HLL_4,    LIST
//   1     0001      HLL_4,     SET
//   2     0010      HLL_4,     HLL
//   4     0100      HLL_6,    LIST
//   5     0101      HLL_6,     SET
//   6     0110      HLL_6,     HLL
//   8     1000      HLL_8,    LIST
//   9     1001      HLL_8,     SET
//  10     1010      HLL_8,     HLL
template<typename A>
uint8_t HllSketchImpl<A>::makeModeByte() const {
  uint8_t byte = 0;

  switch (mode_) {
  case LIST:
    byte = 0;
    break;
  case SET:
    byte = 1;
    break;
  case HLL:
    byte = 2;
    break;
  }

  switch (tgtHllType_) {
  case HLL_4:
    byte |= (0 << 2);  // for completeness
    break;
  case HLL_6:
    byte |= (1 << 2);
    break;
  case HLL_8:
    byte |= (2 << 2); 
    break;
  }

  return byte;
}

template<typename A>
HllSketchImpl<A>* HllSketchImpl<A>::reset() {
  return HllSketchImplFactory<A>::reset(this, startFullSize_);
}

template<typename A>
target_hll_type HllSketchImpl<A>::getTgtHllType() const {
  return tgtHllType_;
}

template<typename A>
uint8_t HllSketchImpl<A>::getLgConfigK() const {
  return lgConfigK_;
}

template<typename A>
hll_mode HllSketchImpl<A>::getCurMode() const {
  return mode_;
}

template<typename A>
bool HllSketchImpl<A>::isStartFullSize() const {
  return startFullSize_;
}

}

#endif // _HLLSKETCHIMPL_INTERNAL_HPP_
