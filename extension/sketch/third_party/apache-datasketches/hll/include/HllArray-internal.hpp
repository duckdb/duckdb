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

#ifndef _HLLARRAY_INTERNAL_HPP_
#define _HLLARRAY_INTERNAL_HPP_

#include "HllArray.hpp"
#include "HllUtil.hpp"
#include "HarmonicNumbers.hpp"
#include "CubicInterpolation.hpp"
#include "CompositeInterpolationXTable.hpp"
#include "CouponList.hpp"
#include "inv_pow2_table.hpp"
#include <cstring>
#include <cmath>
#include <stdexcept>
#include <string>

namespace datasketches {

template<typename A>
HllArray<A>::HllArray(uint8_t lgConfigK, target_hll_type tgtHllType, bool startFullSize, const A& allocator):
HllSketchImpl<A>(lgConfigK, tgtHllType, hll_mode::HLL, startFullSize),
hipAccum_(0.0),
kxq0_(1 << lgConfigK),
kxq1_(0.0),
hllByteArr_(allocator),
curMin_(0),
numAtCurMin_(1 << lgConfigK),
oooFlag_(false)
{}

template<typename A>
HllArray<A>* HllArray<A>::copyAs(target_hll_type tgtHllType) const {
  if (tgtHllType == this->getTgtHllType()) {
    return static_cast<HllArray*>(copy());
  }
  if (tgtHllType == target_hll_type::HLL_4) {
    return HllSketchImplFactory<A>::convertToHll4(*this);
  } else if (tgtHllType == target_hll_type::HLL_6) {
    return HllSketchImplFactory<A>::convertToHll6(*this);
  } else { // tgtHllType == HLL_8
    return HllSketchImplFactory<A>::convertToHll8(*this);
  }
}

template<typename A>
HllArray<A>* HllArray<A>::newHll(const void* bytes, size_t len, const A& allocator) {
  if (len < hll_constants::HLL_BYTE_ARR_START) {
    throw std::out_of_range("Input data length insufficient to hold HLL array");
  }

  const uint8_t* data = static_cast<const uint8_t*>(bytes);
  if (data[hll_constants::PREAMBLE_INTS_BYTE] != hll_constants::HLL_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (data[hll_constants::SER_VER_BYTE] != hll_constants::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (data[hll_constants::FAMILY_BYTE] != hll_constants::FAMILY_ID) {
    throw std::invalid_argument("Input array is not an HLL sketch");
  }

  const hll_mode mode = HllSketchImpl<A>::extractCurMode(data[hll_constants::MODE_BYTE]);
  if (mode != HLL) {
    throw std::invalid_argument("Calling HLL array constructor with non-HLL mode data");
  }

  const target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(data[hll_constants::MODE_BYTE]);
  const bool oooFlag = ((data[hll_constants::FLAGS_BYTE] & hll_constants::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  const bool comapctFlag = ((data[hll_constants::FLAGS_BYTE] & hll_constants::COMPACT_FLAG_MASK) ? true : false);
  const bool startFullSizeFlag = ((data[hll_constants::FLAGS_BYTE] & hll_constants::FULL_SIZE_FLAG_MASK) ? true : false);

  const uint8_t lgK = data[hll_constants::LG_K_BYTE];
  const uint8_t curMin = data[hll_constants::HLL_CUR_MIN_BYTE];

  const uint32_t arrayBytes = hllArrBytes(tgtHllType, lgK);
  if (len < static_cast<size_t>(hll_constants::HLL_BYTE_ARR_START + arrayBytes)) {
    throw std::out_of_range("Input array too small to hold sketch image");
  }

  double hip, kxq0, kxq1;
  std::memcpy(&hip, data + hll_constants::HIP_ACCUM_DOUBLE, sizeof(double));
  std::memcpy(&kxq0, data + hll_constants::KXQ0_DOUBLE, sizeof(double));
  std::memcpy(&kxq1, data + hll_constants::KXQ1_DOUBLE, sizeof(double));

  uint32_t numAtCurMin, auxCount;
  std::memcpy(&numAtCurMin, data + hll_constants::CUR_MIN_COUNT_INT, sizeof(int));
  std::memcpy(&auxCount, data + hll_constants::AUX_COUNT_INT, sizeof(int));

  AuxHashMap<A>* auxHashMap = nullptr;
  typedef std::unique_ptr<AuxHashMap<A>, std::function<void(AuxHashMap<A>*)>> aux_hash_map_ptr;
  aux_hash_map_ptr aux_ptr;
  if (auxCount > 0) { // necessarily TgtHllType == HLL_4
    uint8_t auxLgIntArrSize = data[4];
    const size_t offset = hll_constants::HLL_BYTE_ARR_START + arrayBytes;
    const uint8_t* auxDataStart = data + offset;
    auxHashMap = AuxHashMap<A>::deserialize(auxDataStart, len - offset, lgK, auxCount, auxLgIntArrSize, comapctFlag, allocator);
    aux_ptr = aux_hash_map_ptr(auxHashMap, auxHashMap->make_deleter());
  }

  HllArray<A>* sketch = HllSketchImplFactory<A>::newHll(lgK, tgtHllType, startFullSizeFlag, allocator);
  sketch->putCurMin(curMin);
  sketch->putOutOfOrderFlag(oooFlag);
  if (!oooFlag) sketch->putHipAccum(hip);
  sketch->putKxQ0(kxq0);
  sketch->putKxQ1(kxq1);
  sketch->putNumAtCurMin(numAtCurMin);

  std::memcpy(sketch->hllByteArr_.data(), data + hll_constants::HLL_BYTE_ARR_START, arrayBytes);

  if (auxHashMap != nullptr)
    ((Hll4Array<A>*)sketch)->putAuxHashMap(auxHashMap);

  aux_ptr.release();
  return sketch;
}

template<typename A>
HllArray<A>* HllArray<A>::newHll(std::istream& is, const A& allocator) {
  uint8_t listHeader[8];
  read(is, listHeader, 8 * sizeof(uint8_t));

  if (listHeader[hll_constants::PREAMBLE_INTS_BYTE] != hll_constants::HLL_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (listHeader[hll_constants::SER_VER_BYTE] != hll_constants::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (listHeader[hll_constants::FAMILY_BYTE] != hll_constants::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  hll_mode mode = HllSketchImpl<A>::extractCurMode(listHeader[hll_constants::MODE_BYTE]);
  if (mode != HLL) {
    throw std::invalid_argument("Calling HLL construtor with non-HLL mode data");
  }

  const target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(listHeader[hll_constants::MODE_BYTE]);
  const bool oooFlag = ((listHeader[hll_constants::FLAGS_BYTE] & hll_constants::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  const bool comapctFlag = ((listHeader[hll_constants::FLAGS_BYTE] & hll_constants::COMPACT_FLAG_MASK) ? true : false);
  const bool startFullSizeFlag = ((listHeader[hll_constants::FLAGS_BYTE] & hll_constants::FULL_SIZE_FLAG_MASK) ? true : false);

  const uint8_t lgK = listHeader[hll_constants::LG_K_BYTE];
  const uint8_t curMin = listHeader[hll_constants::HLL_CUR_MIN_BYTE];

  HllArray* sketch = HllSketchImplFactory<A>::newHll(lgK, tgtHllType, startFullSizeFlag, allocator);
  typedef std::unique_ptr<HllArray<A>, std::function<void(HllSketchImpl<A>*)>> hll_array_ptr;
  hll_array_ptr sketch_ptr(sketch, sketch->get_deleter());
  sketch->putCurMin(curMin);
  sketch->putOutOfOrderFlag(oooFlag);

  const auto hip = read<double>(is);
  const auto kxq0 = read<double>(is);
  const auto kxq1 = read<double>(is);
  if (!oooFlag) sketch->putHipAccum(hip);
  sketch->putKxQ0(kxq0);
  sketch->putKxQ1(kxq1);

  const auto numAtCurMin = read<uint32_t>(is);
  const auto auxCount = read<uint32_t>(is);
  sketch->putNumAtCurMin(numAtCurMin);
  
  read(is, sketch->hllByteArr_.data(), sketch->getHllByteArrBytes());
  
  if (auxCount > 0) { // necessarily TgtHllType == HLL_4
    uint8_t auxLgIntArrSize = listHeader[4];
    AuxHashMap<A>* auxHashMap = AuxHashMap<A>::deserialize(is, lgK, auxCount, auxLgIntArrSize, comapctFlag, allocator);
    ((Hll4Array<A>*)sketch)->putAuxHashMap(auxHashMap);
  }

  if (!is.good())
    throw std::runtime_error("error reading from std::istream"); 

  return sketch_ptr.release();
}

template<typename A>
vector_u8<A> HllArray<A>::serialize(bool compact, unsigned header_size_bytes) const {
  const size_t sketchSizeBytes = (compact ? getCompactSerializationBytes() : getUpdatableSerializationBytes()) + header_size_bytes;
  vector_u8<A> byteArr(sketchSizeBytes, 0, getAllocator());
  uint8_t* bytes = byteArr.data() + header_size_bytes;
  AuxHashMap<A>* auxHashMap = getAuxHashMap();

  bytes[hll_constants::PREAMBLE_INTS_BYTE] = static_cast<uint8_t>(getPreInts());
  bytes[hll_constants::SER_VER_BYTE] = static_cast<uint8_t>(hll_constants::SER_VER);
  bytes[hll_constants::FAMILY_BYTE] = static_cast<uint8_t>(hll_constants::FAMILY_ID);
  bytes[hll_constants::LG_K_BYTE] = static_cast<uint8_t>(this->lgConfigK_);
  bytes[hll_constants::LG_ARR_BYTE] = static_cast<uint8_t>(auxHashMap == nullptr ? 0 : auxHashMap->getLgAuxArrInts());
  bytes[hll_constants::FLAGS_BYTE] = this->makeFlagsByte(compact);
  bytes[hll_constants::HLL_CUR_MIN_BYTE] = static_cast<uint8_t>(curMin_);
  bytes[hll_constants::MODE_BYTE] = this->makeModeByte();

  std::memcpy(bytes + hll_constants::HIP_ACCUM_DOUBLE, &hipAccum_, sizeof(double));
  std::memcpy(bytes + hll_constants::KXQ0_DOUBLE, &kxq0_, sizeof(double));
  std::memcpy(bytes + hll_constants::KXQ1_DOUBLE, &kxq1_, sizeof(double));
  std::memcpy(bytes + hll_constants::CUR_MIN_COUNT_INT, &numAtCurMin_, sizeof(uint32_t));
  const uint32_t auxCount = (auxHashMap == nullptr ? 0 : auxHashMap->getAuxCount());
  std::memcpy(bytes + hll_constants::AUX_COUNT_INT, &auxCount, sizeof(uint32_t));

  const uint32_t hllByteArrBytes = getHllByteArrBytes();
  std::memcpy(bytes + getMemDataStart(), hllByteArr_.data(), hllByteArrBytes);

  // aux map if HLL_4
  if (this->tgtHllType_ == HLL_4) {
    bytes += getMemDataStart() + hllByteArrBytes; // start of auxHashMap
    if (auxHashMap != nullptr) {
      if (compact) {
        for (const uint32_t coupon: *auxHashMap) {
          std::memcpy(bytes, &coupon, sizeof(coupon));
          bytes += sizeof(coupon);
        }
      } else {
        std::memcpy(bytes, auxHashMap->getAuxIntArr(), auxHashMap->getUpdatableSizeBytes());
      }
    } else if (!compact) {
      // if updatable, we write even if currently unused so the binary can be wrapped
      uint32_t auxBytes = 4 << hll_constants::LG_AUX_ARR_INTS[this->lgConfigK_];
      std::fill_n(bytes, auxBytes, static_cast<uint8_t>(0));
    }
  }

  return byteArr;
}

template<typename A>
void HllArray<A>::serialize(std::ostream& os, bool compact) const {
  // header
  const uint8_t preInts = getPreInts();
  write(os, preInts);
  const uint8_t serialVersion = hll_constants::SER_VER;
  write(os, serialVersion);
  const uint8_t familyId = hll_constants::FAMILY_ID;
  write(os, familyId);
  const uint8_t lgKByte = this->lgConfigK_;
  write(os, lgKByte);

  AuxHashMap<A>* auxHashMap = getAuxHashMap();
  uint8_t lgArrByte = 0;
  if (auxHashMap != nullptr) {
    lgArrByte = auxHashMap->getLgAuxArrInts();
  }
  write(os, lgArrByte);

  const uint8_t flagsByte = this->makeFlagsByte(compact);
  write(os, flagsByte);
  write(os, curMin_);
  const uint8_t modeByte = this->makeModeByte();
  write(os, modeByte);

  // estimator data
  write(os, hipAccum_);
  write(os, kxq0_);
  write(os, kxq1_);

  // array data
  write(os, numAtCurMin_);

  const uint32_t auxCount = (auxHashMap == nullptr ? 0 : auxHashMap->getAuxCount());
  write(os, auxCount);
  write(os, hllByteArr_.data(), getHllByteArrBytes());

  // aux map if HLL_4
  if (this->tgtHllType_ == HLL_4) {
    if (auxHashMap != nullptr) {
      if (compact) {
        for (const uint32_t coupon: *auxHashMap) {
          write(os, coupon);
        }
      } else {
        write(os, auxHashMap->getAuxIntArr(), auxHashMap->getUpdatableSizeBytes());
      }
    } else if (!compact) {
      // if updatable, we write even if currently unused so the binary can be wrapped      
      uint32_t auxBytes = 4 << hll_constants::LG_AUX_ARR_INTS[this->lgConfigK_];
      std::fill_n(std::ostreambuf_iterator<char>(os), auxBytes, static_cast<char>(0));
    }
  }
}

template<typename A>
double HllArray<A>::getEstimate() const {
  if (oooFlag_) {
    return getCompositeEstimate();
  }
  return getHipAccum();
}

// HLL UPPER AND LOWER BOUNDS

/*
 * The upper and lower bounds are not symmetric and thus are treated slightly differently.
 * For the lower bound, when the unique count is <= k, LB >= numNonZeros, where
 * numNonZeros = k - numAtCurMin AND curMin == 0.
 *
 * For HLL6 and HLL8, curMin is always 0 and numAtCurMin is initialized to k and is decremented
 * down for each valid update until it reaches 0, where it stays. Thus, for these two
 * isomorphs, when numAtCurMin = 0, means the true curMin is > 0 and the unique count must be
 * greater than k.
 *
 * HLL4 always maintains both curMin and numAtCurMin dynamically. Nonetheless, the rules for
 * the very small values <= k where curMin = 0 still apply.
 */
template<typename A>
double HllArray<A>::getLowerBound(uint8_t numStdDev) const {
  HllUtil<A>::checkNumStdDev(numStdDev);
  const uint32_t configK = 1 << this->lgConfigK_;
  const double numNonZeros = ((curMin_ == 0) ? (configK - numAtCurMin_) : configK);

  double estimate;
  double rseFactor;
  if (oooFlag_) {
    estimate = getCompositeEstimate();
    rseFactor = hll_constants::HLL_NON_HIP_RSE_FACTOR;
  } else {
    estimate = hipAccum_;
    rseFactor = hll_constants::HLL_HIP_RSE_FACTOR;
  }

  double relErr;
  if (this->lgConfigK_ > 12) {
    relErr = (numStdDev * rseFactor) / sqrt(configK);
  } else {
    relErr = HllUtil<A>::getRelErr(false, oooFlag_, this->lgConfigK_, numStdDev);
  }
  return fmax(estimate / (1.0 + relErr), numNonZeros);
}

template<typename A>
double HllArray<A>::getUpperBound(uint8_t numStdDev) const {
  HllUtil<A>::checkNumStdDev(numStdDev);
  const uint32_t configK = 1 << this->lgConfigK_;

  double estimate;
  double rseFactor;
  if (oooFlag_) {
    estimate = getCompositeEstimate();
    rseFactor = hll_constants::HLL_NON_HIP_RSE_FACTOR;
  } else {
    estimate = hipAccum_;
    rseFactor = hll_constants::HLL_HIP_RSE_FACTOR;
  }

  double relErr;
  if (this->lgConfigK_ > 12) {
    relErr = (-1.0) * (numStdDev * rseFactor) / sqrt(configK);
  } else {
    relErr = HllUtil<A>::getRelErr(true, oooFlag_, this->lgConfigK_, numStdDev);
  }
  return estimate / (1.0 + relErr);
}

/**
 * This is the (non-HIP) estimator.
 * It is called "composite" because multiple estimators are pasted together.
 * @param absHllArr an instance of the AbstractHllArray class.
 * @return the composite estimate
 */
// Original C: again-two-registers.c hhb_get_composite_estimate L1489
template<typename A>
double HllArray<A>::getCompositeEstimate() const {
  const double rawEst = getHllRawEstimate();

  const double* xArr = CompositeInterpolationXTable<A>::get_x_arr(this->lgConfigK_);
  const uint32_t xArrLen = CompositeInterpolationXTable<A>::get_x_arr_length();
  const double yStride = CompositeInterpolationXTable<A>::get_y_stride(this->lgConfigK_);

  if (rawEst < xArr[0]) {
    return 0;
  }

  const uint32_t xArrLenM1 = xArrLen - 1;

  if (rawEst > xArr[xArrLenM1]) {
    const double finalY = yStride * xArrLenM1;
    const double factor = finalY / xArr[xArrLenM1];
    return rawEst * factor;
  }

  double adjEst = CubicInterpolation<A>::usingXArrAndYStride(xArr, xArrLen, yStride, rawEst);

  // We need to completely avoid the linear_counting estimator if it might have a crazy value.
  // Empirical evidence suggests that the threshold 3*k will keep us safe if 2^4 <= k <= 2^21.

  if (adjEst > (3 << this->lgConfigK_)) { return adjEst; }

  const double linEst = getHllBitMapEstimate();

  // Bias is created when the value of an estimator is compared with a threshold to decide whether
  // to use that estimator or a different one.
  // We conjecture that less bias is created when the average of the two estimators
  // is compared with the threshold. Empirical measurements support this conjecture.

  const double avgEst = (adjEst + linEst) / 2.0;

  // The following constants comes from empirical measurements of the crossover point
  // between the average error of the linear estimator and the adjusted hll estimator
  double crossOver = 0.64;
  if (this->lgConfigK_ == 4)      { crossOver = 0.718; }
  else if (this->lgConfigK_ == 5) { crossOver = 0.672; }

  return (avgEst > (crossOver * (1 << this->lgConfigK_))) ? adjEst : linEst;
}

template<typename A>
double HllArray<A>::getKxQ0() const {
  return kxq0_;
}

template<typename A>
double HllArray<A>::getKxQ1() const {
  return kxq1_;
}

template<typename A>
double HllArray<A>::getHipAccum() const {
  return hipAccum_;
}

template<typename A>
uint8_t HllArray<A>::getCurMin() const {
  return curMin_;
}

template<typename A>
uint32_t HllArray<A>::getNumAtCurMin() const {
  return numAtCurMin_;
}

template<typename A>
void HllArray<A>::putKxQ0(double kxq0) {
  kxq0_ = kxq0;
}

template<typename A>
void HllArray<A>::putKxQ1(double kxq1) {
  kxq1_ = kxq1;
}

template<typename A>
void HllArray<A>::putHipAccum(double hipAccum) {
  hipAccum_ = hipAccum;
}

template<typename A>
void HllArray<A>::putCurMin(uint8_t curMin) {
  curMin_ = curMin;
}

template<typename A>
void HllArray<A>::putNumAtCurMin(uint32_t numAtCurMin) {
  numAtCurMin_ = numAtCurMin;
}

template<typename A>
void HllArray<A>::decNumAtCurMin() {
  --numAtCurMin_;
}

template<typename A>
void HllArray<A>::addToHipAccum(double delta) {
  hipAccum_ += delta;
}

template<typename A>
bool HllArray<A>::isCompact() const {
  return false;
}

template<typename A>
bool HllArray<A>::isEmpty() const {
  const uint32_t configK = 1 << this->lgConfigK_;
  return (getCurMin() == 0) && (getNumAtCurMin() == configK);
}

template<typename A>
void HllArray<A>::putOutOfOrderFlag(bool flag) {
  oooFlag_ = flag;
}

template<typename A>
bool HllArray<A>::isOutOfOrderFlag() const {
  return oooFlag_;
}

template<typename A>
uint32_t HllArray<A>::hllArrBytes(target_hll_type tgtHllType, uint8_t lgConfigK) {
  switch (tgtHllType) {
  case HLL_4:
    return hll4ArrBytes(lgConfigK);
  case HLL_6:
    return hll6ArrBytes(lgConfigK);
  case HLL_8:
    return hll8ArrBytes(lgConfigK);
  default:
    throw std::invalid_argument("Invalid target HLL type"); 
  }
}

template<typename A>
uint32_t HllArray<A>::hll4ArrBytes(uint8_t lgConfigK) {
  return 1 << (lgConfigK - 1);
}

template<typename A>
uint32_t HllArray<A>::hll6ArrBytes(uint8_t lgConfigK) {
  const uint32_t numSlots = 1 << lgConfigK;
  return ((numSlots * 3) >> 2) + 1;
}

template<typename A>
uint32_t HllArray<A>::hll8ArrBytes(uint8_t lgConfigK) {
  return 1 << lgConfigK;
}

template<typename A>
uint32_t HllArray<A>::getMemDataStart() const {
  return hll_constants::HLL_BYTE_ARR_START;
}

template<typename A>
uint32_t HllArray<A>::getUpdatableSerializationBytes() const {
  return hll_constants::HLL_BYTE_ARR_START + getHllByteArrBytes();
}

template<typename A>
uint32_t HllArray<A>::getCompactSerializationBytes() const {
  AuxHashMap<A>* auxHashMap = getAuxHashMap();
  const uint32_t auxCountBytes = ((auxHashMap == nullptr) ? 0 : auxHashMap->getCompactSizeBytes());
  return hll_constants::HLL_BYTE_ARR_START + getHllByteArrBytes() + auxCountBytes;
}

template<typename A>
uint8_t HllArray<A>::getPreInts() const {
  return hll_constants::HLL_PREINTS;
}

template<typename A>
AuxHashMap<A>* HllArray<A>::getAuxHashMap() const {
  return nullptr;
}

template<typename A>
void HllArray<A>::hipAndKxQIncrementalUpdate(uint8_t oldValue, uint8_t newValue) {
  const uint32_t configK = 1 << this->getLgConfigK();
  // update hip BEFORE updating kxq
  if (!oooFlag_) hipAccum_ += configK / (kxq0_ + kxq1_);
  // update kxq0 and kxq1; subtract first, then add
  if (oldValue < 32) { kxq0_ -= INVERSE_POWERS_OF_2[oldValue]; }
  else               { kxq1_ -= INVERSE_POWERS_OF_2[oldValue]; }
  if (newValue < 32) { kxq0_ += INVERSE_POWERS_OF_2[newValue]; }
  else               { kxq1_ += INVERSE_POWERS_OF_2[newValue]; }
}

/**
 * Estimator when N is small, roughly less than k log(k).
 * Refer to Wikipedia: Coupon Collector Problem
 * @return the very low range estimate
 */
//In C: again-two-registers.c hhb_get_improved_linear_counting_estimate L1274
template<typename A>
double HllArray<A>::getHllBitMapEstimate() const {
  const uint32_t configK = 1 << this->lgConfigK_;
  const uint32_t numUnhitBuckets = curMin_ == 0 ? numAtCurMin_ : 0;

  //This will eventually go away.
  if (numUnhitBuckets == 0) {
    return configK * log(configK / 0.5);
  }

  const uint32_t numHitBuckets = configK - numUnhitBuckets;
  return HarmonicNumbers<A>::getBitMapEstimate(configK, numHitBuckets);
}

//In C: again-two-registers.c hhb_get_raw_estimate L1167
template<typename A>
double HllArray<A>::getHllRawEstimate() const {
  const uint32_t configK = 1 << this->lgConfigK_;
  double correctionFactor;
  if (this->lgConfigK_ == 4) { correctionFactor = 0.673; }
  else if (this->lgConfigK_ == 5) { correctionFactor = 0.697; }
  else if (this->lgConfigK_ == 6) { correctionFactor = 0.709; }
  else { correctionFactor = 0.7213 / (1.0 + (1.079 / configK)); }
  const double hyperEst = (correctionFactor * configK * configK) / (kxq0_ + kxq1_);
  return hyperEst;
}

template<typename A>
typename HllArray<A>::const_iterator HllArray<A>::begin(bool all) const {
  return const_iterator(hllByteArr_.data(), 1 << this->lgConfigK_, 0, this->tgtHllType_, nullptr, 0, all);
}

template<typename A>
typename HllArray<A>::const_iterator HllArray<A>::end() const {
  return const_iterator(hllByteArr_.data(), 1 << this->lgConfigK_, 1 << this->lgConfigK_, this->tgtHllType_, nullptr, 0, false);
}

template<typename A>
HllArray<A>::const_iterator::const_iterator(const uint8_t* array, uint32_t array_size, uint32_t index, target_hll_type hll_type, const AuxHashMap<A>* exceptions, uint8_t offset, bool all):
array_(array), array_size_(array_size), index_(index), hll_type_(hll_type), exceptions_(exceptions), offset_(offset), all_(all)
{
  while (index_ < array_size_) {
    value_ = get_value(array_, index_, hll_type_, exceptions_, offset_);
    if (all_ || value_ != hll_constants::EMPTY) break;
    ++index_;
  }
}

template<typename A>
typename HllArray<A>::const_iterator& HllArray<A>::const_iterator::operator++() {
  while (++index_ < array_size_) {
    value_ = get_value(array_, index_, hll_type_, exceptions_, offset_);
    if (all_ || value_ != hll_constants::EMPTY) break;
  }
  return *this;
}

template<typename A>
bool HllArray<A>::const_iterator::operator!=(const const_iterator& other) const {
  return index_ != other.index_;
}

template<typename A>
uint32_t HllArray<A>::const_iterator::operator*() const {
  return HllUtil<A>::pair(index_, value_);
}

template<typename A>
uint8_t HllArray<A>::const_iterator::get_value(const uint8_t* array, uint32_t index, target_hll_type hll_type, const AuxHashMap<A>* exceptions, uint8_t offset) {
  if (hll_type == target_hll_type::HLL_4) {
    uint8_t value = array[index >> 1];
    if ((index & 1) > 0) { // odd
        value >>= 4;
    } else {
      value &= hll_constants::loNibbleMask;
    }
    if (value == hll_constants::AUX_TOKEN) { // exception
      return exceptions->mustFindValueFor(index);
    }
    return value + offset;
  } else if (hll_type == target_hll_type::HLL_6) {
    const size_t start_bit = index * 6;
    const uint8_t shift = start_bit & 0x7;
    const size_t byte_idx = start_bit >> 3;
    const uint16_t two_byte_val = (array[byte_idx + 1] << 8) | array[byte_idx];
    return (two_byte_val >> shift) & hll_constants::VAL_MASK_6;
  }
  // HLL_8
  return array[index];
}

template<typename A>
A HllArray<A>::getAllocator() const {
  return hllByteArr_.get_allocator();
}

}

#endif // _HLLARRAY_INTERNAL_HPP_
