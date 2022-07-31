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

#ifndef _HLL4ARRAY_INTERNAL_HPP_
#define _HLL4ARRAY_INTERNAL_HPP_

#include "Hll4Array.hpp"

#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

namespace datasketches {

template<typename A>
Hll4Array<A>::Hll4Array(uint8_t lgConfigK, bool startFullSize, const A& allocator):
HllArray<A>(lgConfigK, target_hll_type::HLL_4, startFullSize, allocator),
auxHashMap_(nullptr)
{
  const uint32_t numBytes = this->hll4ArrBytes(lgConfigK);
  this->hllByteArr_.resize(numBytes, 0);
}

template<typename A>
Hll4Array<A>::Hll4Array(const Hll4Array<A>& that) :
  HllArray<A>(that)
{
  // can determine hllByteArr size in parent class, no need to allocate here
  // but parent class doesn't handle the auxHashMap
  if (that.auxHashMap_ != nullptr) {
    auxHashMap_ = that.auxHashMap_->copy();
  } else {
    auxHashMap_ = nullptr;
  }
}

template<typename A>
Hll4Array<A>::~Hll4Array() {
  // hllByteArr deleted in parent
  if (auxHashMap_ != nullptr) {
    AuxHashMap<A>::make_deleter()(auxHashMap_);
  }
}

template<typename A>
std::function<void(HllSketchImpl<A>*)> Hll4Array<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    Hll4Array<A>* hll = static_cast<Hll4Array<A>*>(ptr);
    using Hll4Alloc = typename std::allocator_traits<A>::template rebind_alloc<Hll4Array<A>>;
    Hll4Alloc hll4Alloc(hll->getAllocator());
    hll->~Hll4Array();
    hll4Alloc.deallocate(hll, 1);
  };
}

template<typename A>
Hll4Array<A>* Hll4Array<A>::copy() const {
  using Hll4Alloc = typename std::allocator_traits<A>::template rebind_alloc<Hll4Array<A>>;
  Hll4Alloc hll4Alloc(this->getAllocator());
  return new (hll4Alloc.allocate(1)) Hll4Array<A>(*this);
}

template<typename A>
uint32_t Hll4Array<A>::getUpdatableSerializationBytes() const {
  AuxHashMap<A>* auxHashMap = getAuxHashMap();
  uint32_t auxBytes;
  if (auxHashMap == nullptr) {
    auxBytes = 4 << hll_constants::LG_AUX_ARR_INTS[this->lgConfigK_];
  } else {
    auxBytes = 4 << auxHashMap->getLgAuxArrInts();
  }
  return hll_constants::HLL_BYTE_ARR_START + getHllByteArrBytes() + auxBytes;
}

template<typename A>
uint32_t Hll4Array<A>::getHllByteArrBytes() const {
  return this->hll4ArrBytes(this->lgConfigK_);
}

template<typename A>
AuxHashMap<A>* Hll4Array<A>::getAuxHashMap() const {
  return auxHashMap_;
}

template<typename A>
void Hll4Array<A>::putAuxHashMap(AuxHashMap<A>* auxHashMap) {
  this->auxHashMap_ = auxHashMap;
}

template<typename A>
uint8_t Hll4Array<A>::getSlot(uint32_t slotNo) const {
  const uint8_t byte = this->hllByteArr_[slotNo >> 1];
  if ((slotNo & 1) > 0) { // odd?
    return byte >> 4;
  }
  return byte & hll_constants::loNibbleMask;
}

template<typename A>
uint8_t Hll4Array<A>::get_value(uint32_t index) const {
  const uint8_t value = getSlot(index);
  if (value != hll_constants::AUX_TOKEN) return value + this->curMin_;
  return auxHashMap_->mustFindValueFor(index);
}

template<typename A>
HllSketchImpl<A>* Hll4Array<A>::couponUpdate(uint32_t coupon) {
  internalCouponUpdate(coupon);
  return this;
}

template<typename A>
void Hll4Array<A>::internalCouponUpdate(uint32_t coupon) {
  const uint8_t newValue = HllUtil<A>::getValue(coupon);
  if (newValue <= this->curMin_) {
    return; // quick rejection, but only works for large N
  }
  const uint32_t configKmask = (1 << this->lgConfigK_) - 1;
  const uint32_t slotNo = HllUtil<A>::getLow26(coupon) & configKmask;
  internalHll4Update(slotNo, newValue);
}

template<typename A>
void Hll4Array<A>::putSlot(uint32_t slotNo, uint8_t newValue) {
  const uint32_t byteno = slotNo >> 1;
  const uint8_t oldValue = this->hllByteArr_[byteno];
  if ((slotNo & 1) == 0) { // set low nibble
    this->hllByteArr_[byteno]
      = ((oldValue & hll_constants::hiNibbleMask) | (newValue & hll_constants::loNibbleMask));
  } else { // set high nibble
    this->hllByteArr_[byteno]
      = ((oldValue & hll_constants::loNibbleMask) | ((newValue << 4) & hll_constants::hiNibbleMask));
  }
}

//In C: two-registers.c Line 836 in "hhb_abstract_set_slot_if_new_value_bigger" non-sparse
template<typename A>
void Hll4Array<A>::internalHll4Update(uint32_t slotNo, uint8_t newVal) {

  const uint8_t rawStoredOldValue = getSlot(slotNo); // could be a 0
  // this is provably a LB:
  const uint8_t lbOnOldValue = rawStoredOldValue + this->curMin_; // lower bound, could be 0

  if (newVal > lbOnOldValue) { // 842
    // Note: if an AUX_TOKEN exists, then auxHashMap must already exist
    // 846: rawStoredOldValue == AUX_TOKEN
    const uint8_t actualOldValue = (rawStoredOldValue < hll_constants::AUX_TOKEN)
       ? (lbOnOldValue) : (auxHashMap_->mustFindValueFor(slotNo));

    if (newVal > actualOldValue) { // 848: actualOldValue could still be 0; newValue > 0
      // we know that the array will change, but we haven't actually updated yet
      this->hipAndKxQIncrementalUpdate(actualOldValue, newVal);

      // newVal >= curMin

      const uint8_t shiftedNewValue = newVal - this->curMin_; // 874
      // redundant since we know newVal >= curMin,
      // and lgConfigK bounds do not allow overflowing an int
      //assert(shiftedNewValue >= 0);

      if (rawStoredOldValue == hll_constants::AUX_TOKEN) { // 879
        // Given that we have an AUX_TOKEN, there are 4 cases for how to
        // actually modify the data structure

        if (shiftedNewValue >= hll_constants::AUX_TOKEN) { // case 1: 881
          // the byte array already contains aux token
          // This is the case where old and new values are both exceptions.
          // The 4-bit array already is AUX_TOKEN, only need to update auxHashMap
          auxHashMap_->mustReplace(slotNo, newVal);
        }
        else { // case 2: 885
          // This is the hypothetical case where the old value is an exception and the new one is not,
          // which is impossible given that curMin has not changed here and newVal > oldValue
        }
      } else { // rawStoredOldValue != AUX_TOKEN
        if (shiftedNewValue >= hll_constants::AUX_TOKEN) { // case 3: 892
          // This is the case where the old value is not an exception and the new value is.
          // The AUX_TOKEN must be stored in the 4-bit array and the new value
          // added to the exception table
          putSlot(slotNo, hll_constants::AUX_TOKEN);
          if (auxHashMap_ == nullptr) {
            auxHashMap_ = AuxHashMap<A>::newAuxHashMap(hll_constants::LG_AUX_ARR_INTS[this->lgConfigK_],
                this->lgConfigK_, this->getAllocator());
          }
          auxHashMap_->mustAdd(slotNo, newVal);
        }
        else { // case 4: 897
          // This is the case where neither the old value nor the new value is an exception.
          // We just overwrite the 4-bit array with the shifted new value.
          putSlot(slotNo, shiftedNewValue);
        }
      }

      // we just increased a pair value, so it might be time to change curMin
      if (actualOldValue == this->curMin_) { // 908
        this->decNumAtCurMin();
        while (this->numAtCurMin_ == 0) {
          shiftToBiggerCurMin(); // increases curMin by 1, builds a new aux table
          // shifts values in 4-bit table and recounts curMin
        }
      }
    } // end newVal <= actualOldValue
  } // end newValue <= lbOnOldValue -> return, no need to update array
}

// This scheme only works with two double registers (2 kxq values).
//   HipAccum, kxq0 and kxq1 remain untouched.
//   This changes curMin, numAtCurMin, hllByteArr and auxMap.
// Entering this routine assumes that all slots have valid values > 0 and <= 15.
// An AuxHashMap must exist if any values in the current hllByteArray are already 15.
// In C: again-two-registers.c Lines 710 "hhb_shift_to_bigger_curmin"
template<typename A>
void Hll4Array<A>::shiftToBiggerCurMin() {
  const uint8_t newCurMin = this->curMin_ + 1;
  const uint32_t configK = 1 << this->lgConfigK_;
  const uint32_t configKmask = configK - 1;

  uint32_t numAtNewCurMin = 0;
  uint32_t numAuxTokens = 0;

  // Walk through the slots of 4-bit array decrementing stored values by one unless it
  // equals AUX_TOKEN, where it is left alone but counted to be checked later.
  // If oldStoredValue is 0 it is an error.
  // If the decremented value is 0, we increment numAtNewCurMin.
  // Because getNibble is masked to 4 bits oldStoredValue can never be > 15 or negative
  for (uint32_t i = 0; i < configK; i++) { //724
    uint8_t oldStoredValue = getSlot(i);
    if (oldStoredValue == 0) {
      throw std::runtime_error("Array slots cannot be 0 at this point.");
    }
    if (oldStoredValue < hll_constants::AUX_TOKEN) {
      putSlot(i, --oldStoredValue);
      if (oldStoredValue == 0) { numAtNewCurMin++; }
    } else { //oldStoredValue == AUX_TOKEN
      numAuxTokens++;
      if (auxHashMap_ == nullptr) {
        throw std::logic_error("auxHashMap cannot be null at this point");
      }
    }
  }

  // If old AuxHashMap exists, walk through it updating some slots and build a new AuxHashMap
  // if needed.
  AuxHashMap<A>* newAuxMap = nullptr;
  if (auxHashMap_ != nullptr) {
    uint32_t slotNum;
    uint8_t oldActualVal;
    uint8_t newShiftedVal;

    for (const auto coupon: *auxHashMap_) {
      slotNum = HllUtil<A>::getLow26(coupon) & configKmask;
      oldActualVal = HllUtil<A>::getValue(coupon);
      newShiftedVal = oldActualVal - newCurMin;
      if (newShiftedVal < 0) {
        throw std::logic_error("oldActualVal < newCurMin when incrementing curMin");
      }

      if (getSlot(slotNum) != hll_constants::AUX_TOKEN) {
        throw std::logic_error("getSlot(slotNum) != AUX_TOKEN for item in auxiliary hash map");
      }
      // Array slot != AUX_TOKEN at getSlot(slotNum);
      if (newShiftedVal < hll_constants::AUX_TOKEN) { // 756
        if (newShiftedVal != 14) {
          throw std::logic_error("newShiftedVal != 14 for item in old auxHashMap despite curMin increment");
        }
        // The former exception value isn't one anymore, so it stays out of new AuxHashMap.
        // Correct the AUX_TOKEN value in the HLL array to the newShiftedVal (14).
        putSlot(slotNum, newShiftedVal);
        numAuxTokens--;
      } else { //newShiftedVal >= AUX_TOKEN
        // the former exception remains an exception, so must be added to the newAuxMap
        if (newAuxMap == nullptr) {
          newAuxMap = AuxHashMap<A>::newAuxHashMap(hll_constants::LG_AUX_ARR_INTS[this->lgConfigK_],
              this->lgConfigK_, this->getAllocator());
        }
        newAuxMap->mustAdd(slotNum, oldActualVal);
      }
    } //end scan of oldAuxMap
  } //end if (auxHashMap != null)
  else { // oldAuxMap == null
    if (numAuxTokens != 0) {
      throw std::logic_error("No auxiliary hash map, but numAuxTokens != 0");
    }
  }

  if (newAuxMap != nullptr) {
    if (newAuxMap->getAuxCount() != numAuxTokens) {
      throw std::runtime_error("Inconsistent counts: auxCount: " + std::to_string(newAuxMap->getAuxCount())
                               + ", HLL tokesn: " + std::to_string(numAuxTokens));
    }
  }

  if (auxHashMap_ != nullptr) {
    AuxHashMap<A>::make_deleter()(auxHashMap_);
  }
  auxHashMap_ = newAuxMap;

  this->curMin_ = newCurMin;
  this->numAtCurMin_ = numAtNewCurMin;
}

template<typename A>
typename HllArray<A>::const_iterator Hll4Array<A>::begin(bool all) const {
  return typename HllArray<A>::const_iterator(this->hllByteArr_.data(), 1 << this->lgConfigK_, 0, this->tgtHllType_,
      auxHashMap_, this->curMin_, all);
}

template<typename A>
typename HllArray<A>::const_iterator Hll4Array<A>::end() const {
  return typename HllArray<A>::const_iterator(this->hllByteArr_.data(), 1 << this->lgConfigK_, 1 << this->lgConfigK_,
      this->tgtHllType_, auxHashMap_, this->curMin_, false);
}

template<typename A>
void Hll4Array<A>::mergeHll(const HllArray<A>& src) {
  for (const auto coupon: src) {
    internalCouponUpdate(coupon);
  }
}

}

#endif // _HLL4ARRAY_INTERNAL_HPP_
