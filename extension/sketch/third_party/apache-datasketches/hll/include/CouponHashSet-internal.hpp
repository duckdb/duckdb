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

#ifndef _COUPONHASHSET_INTERNAL_HPP_
#define _COUPONHASHSET_INTERNAL_HPP_

#include "CouponHashSet.hpp"

#include <cstring>
#include <exception>
#include <stdexcept>

namespace datasketches {

template<typename A>
static int32_t find(const uint32_t* array, uint8_t lgArrInts, uint32_t coupon);

template<typename A>
CouponHashSet<A>::CouponHashSet(uint8_t lgConfigK, target_hll_type tgtHllType, const A& allocator)
  : CouponList<A>(lgConfigK, tgtHllType, hll_mode::SET, allocator)
{
  if (lgConfigK <= 7) {
    throw std::invalid_argument("CouponHashSet must be initialized with lgConfigK > 7. Found: "
                                + std::to_string(lgConfigK));
  }
}

template<typename A>
CouponHashSet<A>::CouponHashSet(const CouponHashSet<A>& that, const target_hll_type tgtHllType)
  : CouponList<A>(that, tgtHllType) {}

template<typename A>
std::function<void(HllSketchImpl<A>*)> CouponHashSet<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    CouponHashSet<A>* chs = static_cast<CouponHashSet<A>*>(ptr);
    ChsAlloc chsa(chs->getAllocator());
    chs->~CouponHashSet();
    chsa.deallocate(chs, 1);
  };
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::newSet(const void* bytes, size_t len, const A& allocator) {
  if (len < hll_constants::HASH_SET_INT_ARR_START) { // hard-coded
    throw std::out_of_range("Input data length insufficient to hold CouponHashSet");
  }

  const uint8_t* data = static_cast<const uint8_t*>(bytes);
  if (data[hll_constants::PREAMBLE_INTS_BYTE] != hll_constants::HASH_SET_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (data[hll_constants::SER_VER_BYTE] != hll_constants::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (data[hll_constants::FAMILY_BYTE] != hll_constants::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  const hll_mode mode = HllSketchImpl<A>::extractCurMode(data[hll_constants::MODE_BYTE]);
  if (mode != SET) {
    throw std::invalid_argument("Calling set constructor with non-set mode data");
  }

  const target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(data[hll_constants::MODE_BYTE]);

  const uint8_t lgK = data[hll_constants::LG_K_BYTE];
  if (lgK <= 7) {
    throw std::invalid_argument("Attempt to deserialize invalid CouponHashSet with lgConfigK <= 7. Found: "
                                + std::to_string(lgK));
  }   
  uint8_t lgArrInts = data[hll_constants::LG_ARR_BYTE];
  const bool compactFlag = ((data[hll_constants::FLAGS_BYTE] & hll_constants::COMPACT_FLAG_MASK) ? true : false);

  uint32_t couponCount;
  std::memcpy(&couponCount, data + hll_constants::HASH_SET_COUNT_INT, sizeof(couponCount));
  if (lgArrInts < hll_constants::LG_INIT_SET_SIZE) {
    lgArrInts = HllUtil<>::computeLgArrInts(SET, couponCount, lgK);
  }
  // Don't set couponCount in sketch here;
  // we'll set later if updatable, and increment with updates if compact
  const uint32_t couponsInArray = (compactFlag ? couponCount : (1 << lgArrInts));
  const size_t expectedLength = hll_constants::HASH_SET_INT_ARR_START + (couponsInArray * sizeof(uint32_t));
  if (len < expectedLength) {
    throw std::out_of_range("Byte array too short for sketch. Expected " + std::to_string(expectedLength)
                                + ", found: " + std::to_string(len));
  }

  ChsAlloc chsa(allocator);
  CouponHashSet<A>* sketch = new (chsa.allocate(1)) CouponHashSet<A>(lgK, tgtHllType, allocator);

  if (compactFlag) {
    const uint8_t* curPos = data + hll_constants::HASH_SET_INT_ARR_START;
    uint32_t coupon;
    for (uint32_t i = 0; i < couponCount; ++i, curPos += sizeof(coupon)) {
      std::memcpy(&coupon, curPos, sizeof(coupon));
      sketch->couponUpdate(coupon);
    }
  } else {
    sketch->coupons_.resize(1ULL << lgArrInts);
    sketch->couponCount_ = couponCount;
    std::memcpy(sketch->coupons_.data(),
                data + hll_constants::HASH_SET_INT_ARR_START,
                couponsInArray * sizeof(uint32_t));
  }

  return sketch;
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::newSet(std::istream& is, const A& allocator) {
  uint8_t listHeader[8];
  read(is, listHeader, 8 * sizeof(uint8_t));

  if (listHeader[hll_constants::PREAMBLE_INTS_BYTE] != hll_constants::HASH_SET_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (listHeader[hll_constants::SER_VER_BYTE] != hll_constants::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (listHeader[hll_constants::FAMILY_BYTE] != hll_constants::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  hll_mode mode = HllSketchImpl<A>::extractCurMode(listHeader[hll_constants::MODE_BYTE]);
  if (mode != SET) {
    throw std::invalid_argument("Calling set constructor with non-set mode data");
  }

  const target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(listHeader[hll_constants::MODE_BYTE]);

  const uint8_t lgK = listHeader[hll_constants::LG_K_BYTE];
  if (lgK <= 7) {
    throw std::invalid_argument("Attempt to deserialize invalid CouponHashSet with lgConfigK <= 7. Found: "
                                + std::to_string(lgK));
  }
  uint8_t lgArrInts = listHeader[hll_constants::LG_ARR_BYTE];
  const bool compactFlag = ((listHeader[hll_constants::FLAGS_BYTE] & hll_constants::COMPACT_FLAG_MASK) ? true : false);

  const auto couponCount = read<uint32_t>(is);
  if (lgArrInts < hll_constants::LG_INIT_SET_SIZE) {
    lgArrInts = HllUtil<>::computeLgArrInts(SET, couponCount, lgK);
  }

  ChsAlloc chsa(allocator);
  CouponHashSet<A>* sketch = new (chsa.allocate(1)) CouponHashSet<A>(lgK, tgtHllType, allocator);
  typedef std::unique_ptr<CouponHashSet<A>, std::function<void(HllSketchImpl<A>*)>> coupon_hash_set_ptr;
  coupon_hash_set_ptr ptr(sketch, sketch->get_deleter());

  // Don't set couponCount here;
  // we'll set later if updatable, and increment with updates if compact
  if (compactFlag) {
    for (uint32_t i = 0; i < couponCount; ++i) {
      const auto coupon = read<uint32_t>(is);
      sketch->couponUpdate(coupon);
    }
  } else {
    sketch->coupons_.resize(1ULL << lgArrInts);
    sketch->couponCount_ = couponCount;
    // for stream processing, read entire list so read pointer ends up set correctly
    read(is, sketch->coupons_.data(), sketch->coupons_.size() * sizeof(uint32_t));
  } 

  if (!is.good())
    throw std::runtime_error("error reading from std::istream"); 

  return ptr.release();
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::copy() const {
  ChsAlloc chsa(this->coupons_.get_allocator());
  return new (chsa.allocate(1)) CouponHashSet<A>(*this);
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::copyAs(target_hll_type tgtHllType) const {
  ChsAlloc chsa(this->coupons_.get_allocator());
  return new (chsa.allocate(1)) CouponHashSet<A>(*this, tgtHllType);
}

template<typename A>
HllSketchImpl<A>* CouponHashSet<A>::couponUpdate(uint32_t coupon) {
  const uint8_t lgCouponArrInts = count_trailing_zeros_in_u32(static_cast<uint32_t>(this->coupons_.size()));
  const int32_t index = find<A>(this->coupons_.data(), lgCouponArrInts, coupon);
  if (index >= 0) {
    return this; // found duplicate, ignore
  }
  this->coupons_[~index] = coupon; // found empty
  ++this->couponCount_;
  if (checkGrowOrPromote()) {
    return this->promoteHeapListOrSetToHll(*this);
  }
  return this;
}

template<typename A>
uint32_t CouponHashSet<A>::getMemDataStart() const {
  return hll_constants::HASH_SET_INT_ARR_START;
}

template<typename A>
uint8_t CouponHashSet<A>::getPreInts() const {
  return hll_constants::HASH_SET_PREINTS;
}

template<typename A>
bool CouponHashSet<A>::checkGrowOrPromote() {
  if (static_cast<size_t>(hll_constants::RESIZE_DENOM * this->couponCount_) > (hll_constants::RESIZE_NUMER * this->coupons_.size())) {
    const uint8_t lgCouponArrInts = count_trailing_zeros_in_u32(static_cast<uint32_t>(this->coupons_.size()));
    if (lgCouponArrInts == (this->lgConfigK_ - 3)) { // at max size
      return true; // promote to HLL
    }
    growHashSet(lgCouponArrInts + 1);
  }
  return false;
}

template<typename A>
void CouponHashSet<A>::growHashSet(uint8_t tgtLgCoupArrSize) {
  const uint32_t tgtLen = 1 << tgtLgCoupArrSize;
  vector_int coupons_new(tgtLen, 0, this->coupons_.get_allocator());

  const uint32_t srcLen = static_cast<uint32_t>(this->coupons_.size());
  for (uint32_t i = 0; i < srcLen; ++i) { // scan existing array for non-zero values
    const uint32_t fetched = this->coupons_[i];
    if (fetched != hll_constants::EMPTY) {
      const int32_t idx = find<A>(coupons_new.data(), tgtLgCoupArrSize, fetched); // search TGT array
      if (idx < 0) { // found EMPTY
        coupons_new[~idx] = fetched; // insert
        continue;
      }
      throw std::runtime_error("Error: Found duplicate coupon");
    }
  }
  this->coupons_ = std::move(coupons_new);
}

template<typename A>
static int32_t find(const uint32_t* array, uint8_t lgArrInts, uint32_t coupon) {
  const uint32_t arrMask = (1 << lgArrInts) - 1;
  uint32_t probe = coupon & arrMask;
  const uint32_t loopIndex = probe;
  do {
    const uint32_t couponAtIdx = array[probe];
    if (couponAtIdx == hll_constants::EMPTY) {
      return ~probe; //empty
    }
    else if (coupon == couponAtIdx) {
      return probe; //duplicate
    }
    const uint32_t stride = ((coupon & hll_constants::KEY_MASK_26) >> lgArrInts) | 1;
    probe = (probe + stride) & arrMask;
  } while (probe != loopIndex);
  throw std::invalid_argument("Key not found and no empty slots!");
}

}

#endif // _COUPONHASHSET_INTERNAL_HPP_
