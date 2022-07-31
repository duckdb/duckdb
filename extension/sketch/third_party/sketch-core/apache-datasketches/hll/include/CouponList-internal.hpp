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

#ifndef _COUPONLIST_INTERNAL_HPP_
#define _COUPONLIST_INTERNAL_HPP_

#include "CouponList.hpp"
#include "CubicInterpolation.hpp"
#include "HllUtil.hpp"
#include "count_zeros.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace datasketches {

template<typename A>
CouponList<A>::CouponList(uint8_t lgConfigK, target_hll_type tgtHllType, hll_mode mode, const A& allocator):
HllSketchImpl<A>(lgConfigK, tgtHllType, mode, false),
couponCount_(0),
oooFlag_(false),
coupons_(1ULL << (mode == hll_mode::LIST ? hll_constants::LG_INIT_LIST_SIZE : hll_constants::LG_INIT_SET_SIZE), 0, allocator)
{}

template<typename A>
CouponList<A>::CouponList(const CouponList& that, const target_hll_type tgtHllType):
HllSketchImpl<A>(that.lgConfigK_, tgtHllType, that.mode_, false),
couponCount_(that.couponCount_),
oooFlag_(that.oooFlag_),
coupons_(that.coupons_)
{}

template<typename A>
std::function<void(HllSketchImpl<A>*)> CouponList<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    CouponList<A>* cl = static_cast<CouponList<A>*>(ptr);
    ClAlloc cla(cl->getAllocator());
    cl->~CouponList();
    cla.deallocate(cl, 1);
  };
}

template<typename A>
CouponList<A>* CouponList<A>::copy() const {
  ClAlloc cla(coupons_.get_allocator());
  return new (cla.allocate(1)) CouponList<A>(*this);
}

template<typename A>
CouponList<A>* CouponList<A>::copyAs(target_hll_type tgtHllType) const {
  ClAlloc cla(coupons_.get_allocator());
  return new (cla.allocate(1)) CouponList<A>(*this, tgtHllType);
}

template<typename A>
CouponList<A>* CouponList<A>::newList(const void* bytes, size_t len, const A& allocator) {
  if (len < hll_constants::LIST_INT_ARR_START) {
    throw std::out_of_range("Input data length insufficient to hold CouponHashSet");
  }

  const uint8_t* data = static_cast<const uint8_t*>(bytes);
  if (data[hll_constants::PREAMBLE_INTS_BYTE] != hll_constants::LIST_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (data[hll_constants::SER_VER_BYTE] != hll_constants::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (data[hll_constants::FAMILY_BYTE] != hll_constants::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  hll_mode mode = HllSketchImpl<A>::extractCurMode(data[hll_constants::MODE_BYTE]);
  if (mode != LIST) {
    throw std::invalid_argument("Calling list constructor with non-list mode data");
  }

  target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(data[hll_constants::MODE_BYTE]);

  const uint8_t lgK = data[hll_constants::LG_K_BYTE];
  const bool compact = ((data[hll_constants::FLAGS_BYTE] & hll_constants::COMPACT_FLAG_MASK) ? true : false);
  const bool oooFlag = ((data[hll_constants::FLAGS_BYTE] & hll_constants::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  const bool emptyFlag = ((data[hll_constants::FLAGS_BYTE] & hll_constants::EMPTY_FLAG_MASK) ? true : false);

  const uint32_t couponCount = data[hll_constants::LIST_COUNT_BYTE];
  const uint32_t couponsInArray = (compact ? couponCount : (1 << HllUtil<A>::computeLgArrInts(LIST, couponCount, lgK)));
  const size_t expectedLength = hll_constants::LIST_INT_ARR_START + (couponsInArray * sizeof(uint32_t));
  if (len < expectedLength) {
    throw std::out_of_range("Byte array too short for sketch. Expected " + std::to_string(expectedLength)
                                + ", found: " + std::to_string(len));
  }

  ClAlloc cla(allocator);
  CouponList<A>* sketch = new (cla.allocate(1)) CouponList<A>(lgK, tgtHllType, mode, allocator);
  sketch->couponCount_ = couponCount;
  sketch->putOutOfOrderFlag(oooFlag); // should always be false for LIST

  if (!emptyFlag) {
    // only need to read valid coupons, unlike in stream case
    std::memcpy(sketch->coupons_.data(), data + hll_constants::LIST_INT_ARR_START, couponCount * sizeof(uint32_t));
  }
  
  return sketch;
}

template<typename A>
CouponList<A>* CouponList<A>::newList(std::istream& is, const A& allocator) {
  uint8_t listHeader[8];
  read(is, listHeader, 8 * sizeof(uint8_t));

  if (listHeader[hll_constants::PREAMBLE_INTS_BYTE] != hll_constants::LIST_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (listHeader[hll_constants::SER_VER_BYTE] != hll_constants::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (listHeader[hll_constants::FAMILY_BYTE] != hll_constants::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  hll_mode mode = HllSketchImpl<A>::extractCurMode(listHeader[hll_constants::MODE_BYTE]);
  if (mode != LIST) {
    throw std::invalid_argument("Calling list constructor with non-list mode data");
  }

  const target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(listHeader[hll_constants::MODE_BYTE]);

  const uint8_t lgK = listHeader[hll_constants::LG_K_BYTE];
  const bool compact = ((listHeader[hll_constants::FLAGS_BYTE] & hll_constants::COMPACT_FLAG_MASK) ? true : false);
  const bool oooFlag = ((listHeader[hll_constants::FLAGS_BYTE] & hll_constants::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  const bool emptyFlag = ((listHeader[hll_constants::FLAGS_BYTE] & hll_constants::EMPTY_FLAG_MASK) ? true : false);

  ClAlloc cla(allocator);
  CouponList<A>* sketch = new (cla.allocate(1)) CouponList<A>(lgK, tgtHllType, mode, allocator);
  using coupon_list_ptr = std::unique_ptr<CouponList<A>, std::function<void(HllSketchImpl<A>*)>>;
  coupon_list_ptr ptr(sketch, sketch->get_deleter());
  const uint32_t couponCount = listHeader[hll_constants::LIST_COUNT_BYTE];
  sketch->couponCount_ = couponCount;
  sketch->putOutOfOrderFlag(oooFlag); // should always be false for LIST

  if (!emptyFlag) {
    // For stream processing, need to read entire number written to stream so read
    // pointer ends up set correctly.
    // If not compact, still need to read empty items even though in order.
    const uint32_t numToRead = (compact ? couponCount : static_cast<uint32_t>(sketch->coupons_.size()));
    read(is, sketch->coupons_.data(), numToRead * sizeof(uint32_t));
  }

  if (!is.good())
    throw std::runtime_error("error reading from std::istream"); 

  return ptr.release();
}

template<typename A>
vector_u8<A> CouponList<A>::serialize(bool compact, unsigned header_size_bytes) const {
  const size_t sketchSizeBytes = (compact ? getCompactSerializationBytes() : getUpdatableSerializationBytes()) + header_size_bytes;
  vector_u8<A> byteArr(sketchSizeBytes, 0, getAllocator());
  uint8_t* bytes = byteArr.data() + header_size_bytes;

  bytes[hll_constants::PREAMBLE_INTS_BYTE] = static_cast<uint8_t>(getPreInts());
  bytes[hll_constants::SER_VER_BYTE] = static_cast<uint8_t>(hll_constants::SER_VER);
  bytes[hll_constants::FAMILY_BYTE] = static_cast<uint8_t>(hll_constants::FAMILY_ID);
  bytes[hll_constants::LG_K_BYTE] = static_cast<uint8_t>(this->lgConfigK_);
  bytes[hll_constants::LG_ARR_BYTE] = count_trailing_zeros_in_u32(static_cast<uint32_t>(coupons_.size()));
  bytes[hll_constants::FLAGS_BYTE] = this->makeFlagsByte(compact);
  bytes[hll_constants::LIST_COUNT_BYTE] = static_cast<uint8_t>(this->mode_ == LIST ? couponCount_ : 0);
  bytes[hll_constants::MODE_BYTE] = this->makeModeByte();

  if (this->mode_ == SET) {
    std::memcpy(bytes + hll_constants::HASH_SET_COUNT_INT, &couponCount_, sizeof(couponCount_));
  }

  // coupons
  // isCompact() is always false for now
  const int sw = (isCompact() ? 2 : 0) | (compact ? 1 : 0);
  switch (sw) {
    case 0: { // src updatable, dst updatable
      std::memcpy(bytes + getMemDataStart(), coupons_.data(), coupons_.size() * sizeof(uint32_t));
      break;
    }
    case 1: { // src updatable, dst compact
      bytes += getMemDataStart(); // reusing pointer for incremental writes
      for (const uint32_t coupon: *this) {
        std::memcpy(bytes, &coupon, sizeof(coupon));
        bytes += sizeof(coupon);
      }
      break;
    }

    default:
      throw std::runtime_error("Impossible condition when serializing");
  }

  return byteArr;
}

template<typename A>
void CouponList<A>::serialize(std::ostream& os, const bool compact) const {
  // header
  const uint8_t preInts = getPreInts();
  write(os, preInts);
  const uint8_t serialVersion(hll_constants::SER_VER);
  write(os, serialVersion);
  const uint8_t familyId(hll_constants::FAMILY_ID);
  write(os, familyId);
  const uint8_t lgKByte = this->lgConfigK_;
  write(os, lgKByte);
  const uint8_t lgArrIntsByte = count_trailing_zeros_in_u32(static_cast<uint32_t>(coupons_.size()));
  write(os, lgArrIntsByte);
  const uint8_t flagsByte = this->makeFlagsByte(compact);
  write(os, flagsByte);

  if (this->mode_ == LIST) {
    const uint8_t listCount = static_cast<uint8_t>(couponCount_);
    write(os, listCount);
  } else { // mode == SET
      const uint8_t unused = 0;
    write(os, unused);
  }

  const uint8_t modeByte = this->makeModeByte();
  write(os, modeByte);

  if (this->mode_ == SET) {
    // writing as int, already stored as int
    write(os, couponCount_);
  }

  // coupons
  // isCompact() is always false for now
  const int sw = (isCompact() ? 2 : 0) | (compact ? 1 : 0);
  switch (sw) {
    case 0: { // src updatable, dst updatable
      write(os, coupons_.data(), coupons_.size() * sizeof(uint32_t));
      break;
    }
    case 1: { // src updatable, dst compact
      for (const uint32_t coupon: *this) {
        write(os, coupon);
      }
      break;
    }

    default:
      throw std::runtime_error("Impossible condition when serializing");
  }
  
  return;
}

template<typename A>
HllSketchImpl<A>* CouponList<A>::couponUpdate(uint32_t coupon) {
  for (size_t i = 0; i < coupons_.size(); ++i) { // search for empty slot
    const uint32_t couponAtIdx = coupons_[i];
    if (couponAtIdx == hll_constants::EMPTY) {
      coupons_[i] = coupon; // the actual update
      ++couponCount_;
      if (couponCount_ == static_cast<uint32_t>(coupons_.size())) { // array full
        if (this->lgConfigK_ < 8) {
          return promoteHeapListOrSetToHll(*this);
        }
        return promoteHeapListToSet(*this);
      }
      return this;
    }
    // cell not empty
    if (couponAtIdx == coupon) {
      return this; // duplicate
    }
    // cell not empty and not a duplicate, continue
  }
  throw std::runtime_error("Array invalid: no empties and no duplicates");
}

template<typename A>
double CouponList<A>::getCompositeEstimate() const { return getEstimate(); }

template<typename A>
double CouponList<A>::getEstimate() const {
  const double est = CubicInterpolation<A>::usingXAndYTables(couponCount_);
  return fmax(est, couponCount_);
}

template<typename A>
double CouponList<A>::getLowerBound(uint8_t numStdDev) const {
  HllUtil<A>::checkNumStdDev(numStdDev);
  const double est = CubicInterpolation<A>::usingXAndYTables(couponCount_);
  const double tmp = est / (1.0 + (numStdDev * hll_constants::COUPON_RSE));
  return fmax(tmp, couponCount_);
}

template<typename A>
double CouponList<A>::getUpperBound(uint8_t numStdDev) const {
  HllUtil<A>::checkNumStdDev(numStdDev);
  const double est = CubicInterpolation<A>::usingXAndYTables(couponCount_);
  const double tmp = est / (1.0 - (numStdDev * hll_constants::COUPON_RSE));
  return fmax(tmp, couponCount_);
}

template<typename A>
bool CouponList<A>::isEmpty() const { return getCouponCount() == 0; }

template<typename A>
uint32_t CouponList<A>::getUpdatableSerializationBytes() const {
  return getMemDataStart() + static_cast<uint32_t>(coupons_.size()) * sizeof(uint32_t);
}

template<typename A>
uint32_t CouponList<A>::getCouponCount() const {
  return couponCount_;
}

template<typename A>
uint32_t CouponList<A>::getCompactSerializationBytes() const {
  return getMemDataStart() + (couponCount_ << 2);
}

template<typename A>
uint32_t CouponList<A>::getMemDataStart() const {
  return hll_constants::LIST_INT_ARR_START;
}

template<typename A>
uint8_t CouponList<A>::getPreInts() const {
  return hll_constants::LIST_PREINTS;
}

template<typename A>
bool CouponList<A>::isCompact() const { return false; }

template<typename A>
bool CouponList<A>::isOutOfOrderFlag() const { return oooFlag_; }

template<typename A>
void CouponList<A>::putOutOfOrderFlag(bool oooFlag) {
  oooFlag_ = oooFlag;
}

template<typename A>
A CouponList<A>::getAllocator() const {
  return coupons_.get_allocator();
}

template<typename A>
HllSketchImpl<A>* CouponList<A>::promoteHeapListToSet(CouponList& list) {
  return HllSketchImplFactory<A>::promoteListToSet(list);
}

template<typename A>
HllSketchImpl<A>* CouponList<A>::promoteHeapListOrSetToHll(CouponList& src) {
  return HllSketchImplFactory<A>::promoteListOrSetToHll(src);
}

template<typename A>
coupon_iterator<A> CouponList<A>::begin(bool all) const {
  return coupon_iterator<A>(coupons_.data(), coupons_.size(), 0, all);
}

template<typename A>
coupon_iterator<A> CouponList<A>::end() const {
  return coupon_iterator<A>(coupons_.data(), coupons_.size(), coupons_.size(), false);
}

}

#endif // _COUPONLIST_INTERNAL_HPP_
