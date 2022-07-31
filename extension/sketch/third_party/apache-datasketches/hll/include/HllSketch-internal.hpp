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

#ifndef _HLLSKETCH_INTERNAL_HPP_
#define _HLLSKETCH_INTERNAL_HPP_

#include "hll.hpp"
#include "HllUtil.hpp"
#include "HllSketchImplFactory.hpp"
#include "CouponList.hpp"
#include "HllArray.hpp"
#include "common_defs.hpp"

#include <cstdio>
#include <cstdlib>
#include <string>
#include <iostream>
#include <sstream>
#include <iomanip>

namespace datasketches {

typedef union {
  int64_t longBytes;
  double doubleBytes;
} longDoubleUnion;

template<typename A>
hll_sketch_alloc<A>::hll_sketch_alloc(uint8_t lg_config_k, target_hll_type tgt_type, bool start_full_size, const A& allocator) {
  HllUtil<A>::checkLgK(lg_config_k);
  if (start_full_size) {
    sketch_impl = HllSketchImplFactory<A>::newHll(lg_config_k, tgt_type, start_full_size, allocator);
  } else {
    typedef typename std::allocator_traits<A>::template rebind_alloc<CouponList<A>> clAlloc;
    sketch_impl = new (clAlloc(allocator).allocate(1)) CouponList<A>(lg_config_k, tgt_type, hll_mode::LIST, allocator);
  }
}

template<typename A>
hll_sketch_alloc<A> hll_sketch_alloc<A>::deserialize(std::istream& is, const A& allocator) {
  HllSketchImpl<A>* impl = HllSketchImplFactory<A>::deserialize(is, allocator);
  return hll_sketch_alloc<A>(impl);
}

template<typename A>
hll_sketch_alloc<A> hll_sketch_alloc<A>::deserialize(const void* bytes, size_t len, const A& allocator) {
  HllSketchImpl<A>* impl = HllSketchImplFactory<A>::deserialize(bytes, len, allocator);
  return hll_sketch_alloc<A>(impl);
}

template<typename A>
hll_sketch_alloc<A>::~hll_sketch_alloc() {
  if (sketch_impl != nullptr) {
    sketch_impl->get_deleter()(sketch_impl);
  }
}

template<typename A>
hll_sketch_alloc<A>::hll_sketch_alloc(const hll_sketch_alloc<A>& that) :
  sketch_impl(that.sketch_impl->copy())
{}

template<typename A>
hll_sketch_alloc<A>::hll_sketch_alloc(const hll_sketch_alloc<A>& that, target_hll_type tgt_type) :
  sketch_impl(that.sketch_impl->copyAs(tgt_type))
{}

template<typename A>
hll_sketch_alloc<A>::hll_sketch_alloc(hll_sketch_alloc<A>&& that) noexcept :
  sketch_impl(nullptr)
{
  std::swap(sketch_impl, that.sketch_impl);
}

template<typename A>
hll_sketch_alloc<A>::hll_sketch_alloc(HllSketchImpl<A>* that) :
  sketch_impl(that)
{}

template<typename A>
hll_sketch_alloc<A> hll_sketch_alloc<A>::operator=(const hll_sketch_alloc<A>& other) {
  sketch_impl->get_deleter()(sketch_impl);
  sketch_impl = other.sketch_impl->copy();
  return *this;
}

template<typename A>
hll_sketch_alloc<A> hll_sketch_alloc<A>::operator=(hll_sketch_alloc<A>&& other) {
  std::swap(sketch_impl, other.sketch_impl);
  return *this;
}

template<typename A>
void hll_sketch_alloc<A>::reset() {
  // TODO: need to allow starting from a full-sized sketch
  //       (either here or in other implementation)
  sketch_impl = sketch_impl->reset();
}

template<typename A>
void hll_sketch_alloc<A>::update(const std::string& datum) {
  if (datum.empty()) { return; }
  HashState hashResult;
  HllUtil<A>::hash(datum.c_str(), datum.length(), DEFAULT_SEED, hashResult);
  coupon_update(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(uint64_t datum) {
  // no sign extension with 64 bits so no need to cast to signed value
  HashState hashResult;
  HllUtil<A>::hash(&datum, sizeof(uint64_t), DEFAULT_SEED, hashResult);
  coupon_update(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(uint32_t datum) {
  update(static_cast<int32_t>(datum));
}

template<typename A>
void hll_sketch_alloc<A>::update(uint16_t datum) {
  update(static_cast<int16_t>(datum));
}

template<typename A>
void hll_sketch_alloc<A>::update(uint8_t datum) {
  update(static_cast<int8_t>(datum));
}

template<typename A>
void hll_sketch_alloc<A>::update(int64_t datum) {
  HashState hashResult;
  HllUtil<A>::hash(&datum, sizeof(int64_t), DEFAULT_SEED, hashResult);
  coupon_update(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(int32_t datum) {
  const int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil<A>::hash(&val, sizeof(int64_t), DEFAULT_SEED, hashResult);
  coupon_update(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(int16_t datum) {
  const int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil<A>::hash(&val, sizeof(int64_t), DEFAULT_SEED, hashResult);
  coupon_update(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(int8_t datum) {
  const int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil<A>::hash(&val, sizeof(int64_t), DEFAULT_SEED, hashResult);
  coupon_update(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(double datum) {
  longDoubleUnion d;
  d.doubleBytes = static_cast<double>(datum);
  if (datum == 0.0) {
    d.doubleBytes = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(d.doubleBytes)) {
    d.longBytes = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  HashState hashResult;
  HllUtil<A>::hash(&d, sizeof(double), DEFAULT_SEED, hashResult);
  coupon_update(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(float datum) {
  longDoubleUnion d;
  d.doubleBytes = static_cast<double>(datum);
  if (datum == 0.0) {
    d.doubleBytes = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(d.doubleBytes)) {
    d.longBytes = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  HashState hashResult;
  HllUtil<A>::hash(&d, sizeof(double), DEFAULT_SEED, hashResult);
  coupon_update(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(const void* data, size_t lengthBytes) {
  if (data == nullptr) { return; }
  HashState hashResult;
  HllUtil<A>::hash(data, lengthBytes, DEFAULT_SEED, hashResult);
  coupon_update(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::coupon_update(uint32_t coupon) {
  if (coupon == hll_constants::EMPTY) { return; }
  HllSketchImpl<A>* result = this->sketch_impl->couponUpdate(coupon);
  if (result != this->sketch_impl) {
    this->sketch_impl->get_deleter()(this->sketch_impl);
    this->sketch_impl = result;
  }
}

template<typename A>
void hll_sketch_alloc<A>::serialize_compact(std::ostream& os) const {
  return sketch_impl->serialize(os, true);
}

template<typename A>
void hll_sketch_alloc<A>::serialize_updatable(std::ostream& os) const {
  return sketch_impl->serialize(os, false);
}

template<typename A>
vector_u8<A> hll_sketch_alloc<A>::serialize_compact(unsigned header_size_bytes) const {
  return sketch_impl->serialize(true, header_size_bytes);
}

template<typename A>
vector_u8<A> hll_sketch_alloc<A>::serialize_updatable() const {
  return sketch_impl->serialize(false, 0);
}

template<typename A>
string<A> hll_sketch_alloc<A>::to_string(const bool summary,
                                         const bool detail,
                                         const bool aux_detail,
                                         const bool all) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::stringstream os;
  if (summary) {
    os << "### HLL sketch summary:" << std::endl
       << "  Log Config K   : " << std::to_string(get_lg_config_k()) << std::endl
       << "  Hll Target     : " << type_as_string() << std::endl
       << "  Current Mode   : " << mode_as_string() << std::endl
       << "  LB             : " << get_lower_bound(1) << std::endl
       << "  Estimate       : " << get_estimate() << std::endl
       << "  UB             : " << get_upper_bound(1) << std::endl
       << "  OutOfOrder flag: " << (is_out_of_order_flag() ? "true" : "false") << std::endl;
    if (get_current_mode() == HLL) {
      HllArray<A>* hllArray = (HllArray<A>*) sketch_impl;
      os << "  CurMin         : " << std::to_string(hllArray->getCurMin()) << std::endl
         << "  NumAtCurMin    : " << hllArray->getNumAtCurMin() << std::endl
         << "  HipAccum       : " << hllArray->getHipAccum() << std::endl
         << "  KxQ0           : " << hllArray->getKxQ0() << std::endl
         << "  KxQ1           : " << hllArray->getKxQ1() << std::endl;
      if (get_target_type() == HLL_4) {
        const Hll4Array<A>* hll4_ptr = static_cast<const Hll4Array<A>*>(sketch_impl);
        os << "  Aux table?     : " << (hll4_ptr->getAuxHashMap() != nullptr ? "true" : "false") << std::endl;
      }
    } else {
      os << "  Coupon count   : "
         << std::to_string(((CouponList<A>*) sketch_impl)->getCouponCount()) << std::endl;
    }
    os << "### End HLL sketch summary" << std::endl;
  }

  if (detail) {
    os << "### HLL sketch data detail:" << std::endl;
    if (get_current_mode() == HLL) {
      const HllArray<A>* hll_ptr = static_cast<const HllArray<A>*>(sketch_impl);
      os << std::left << std::setw(10) << "Slot" << std::setw(6) << "Value" << std::endl;
      auto it = hll_ptr->begin(all);
      while (it != hll_ptr->end()) {
        os << std::setw(10) << HllUtil<A>::getLow26(*it);
        os << std::setw(6) << HllUtil<A>::getValue(*it);
        os << std::endl;
        ++it;
      }
    } else {
      const CouponList<A>* list_ptr = static_cast<const CouponList<A>*>(sketch_impl);
      os << std::left;
      os << std::setw(10) << "Index";
      os << std::setw(10) << "Key";
      os << std::setw(10) << "Slot";
      os << std::setw(6) << "Value";
      os << std::endl;
      auto it = list_ptr->begin(all);
      int i = 0;
      int mask = (1 << get_lg_config_k()) - 1;
      while (it != list_ptr->end()) {
        os << std::setw(10) << i;
        os << std::setw(10) << HllUtil<A>::getLow26(*it);
        os << std::setw(10) << (HllUtil<A>::getLow26(*it) & mask);
        os << std::setw(6) << HllUtil<A>::getValue(*it);
        os << std::endl;
        ++it;
        ++i;
      }
    }
    os << "### End HLL sketch data detail" << std::endl;
  }
  if (aux_detail) {
    if ((get_current_mode() == HLL) && (get_target_type() == HLL_4)) {
      const Hll4Array<A>* hll4_ptr = static_cast<const Hll4Array<A>*>(sketch_impl);
      const AuxHashMap<A>* aux_ptr = hll4_ptr->getAuxHashMap();
      if (aux_ptr != nullptr) {
        os << "### HLL sketch aux detail:" << std::endl;
        os << std::left;
        os << std::setw(10) << "Index";
        os << std::setw(10) << "Key";
        os << std::setw(10) << "Slot";
        os << std::setw(6) << "Value";
        os << std::endl;
        auto it = aux_ptr->begin(all);
        int i = 0;
        int mask = (1 << get_lg_config_k()) - 1;
        while (it != aux_ptr->end()) {
          os << std::setw(10) << i;
          os << std::setw(10) << HllUtil<A>::getLow26(*it);
          os << std::setw(10) << (HllUtil<A>::getLow26(*it) & mask);
          os << std::setw(6) << HllUtil<A>::getValue(*it);
          os << std::endl;
          ++it;
          ++i;
        }
        os << "### End HLL sketch aux detail" << std::endl;
      }
    }
  }

  return string<A>(os.str().c_str(), sketch_impl->getAllocator());
}

template<typename A>
double hll_sketch_alloc<A>::get_estimate() const {
  return sketch_impl->getEstimate();
}

template<typename A>
double hll_sketch_alloc<A>::get_composite_estimate() const {
  return sketch_impl->getCompositeEstimate();
}

template<typename A>
double hll_sketch_alloc<A>::get_lower_bound(uint8_t numStdDev) const {
  return sketch_impl->getLowerBound(numStdDev);
}

template<typename A>
double hll_sketch_alloc<A>::get_upper_bound(uint8_t numStdDev) const {
  return sketch_impl->getUpperBound(numStdDev);
}

template<typename A>
hll_mode hll_sketch_alloc<A>::get_current_mode() const {
  return sketch_impl->getCurMode();
}

template<typename A>
uint8_t hll_sketch_alloc<A>::get_lg_config_k() const {
  return sketch_impl->getLgConfigK();
}

template<typename A>
target_hll_type hll_sketch_alloc<A>::get_target_type() const {
  return sketch_impl->getTgtHllType();
}

template<typename A>
bool hll_sketch_alloc<A>::is_out_of_order_flag() const {
  return sketch_impl->isOutOfOrderFlag();
}

template<typename A>
bool hll_sketch_alloc<A>::is_estimation_mode() const {
  return true;
}

template<typename A>
uint32_t hll_sketch_alloc<A>::get_updatable_serialization_bytes() const {
  return sketch_impl->getUpdatableSerializationBytes();
}

template<typename A>
uint32_t hll_sketch_alloc<A>::get_compact_serialization_bytes() const {
  return sketch_impl->getCompactSerializationBytes();
}

template<typename A>
bool hll_sketch_alloc<A>::is_compact() const {
  return sketch_impl->isCompact();
}

template<typename A>
bool hll_sketch_alloc<A>::is_empty() const {
  return sketch_impl->isEmpty();
}

template<typename A>
std::string hll_sketch_alloc<A>::type_as_string() const {
  switch (sketch_impl->getTgtHllType()) {
    case target_hll_type::HLL_4:
      return std::string("HLL_4");
    case target_hll_type::HLL_6:
      return std::string("HLL_6");
    case target_hll_type::HLL_8:
      return std::string("HLL_8");
    default:
      throw std::runtime_error("Sketch state error: Invalid target_hll_type");
  }
}

template<typename A>
std::string hll_sketch_alloc<A>::mode_as_string() const {
  switch (sketch_impl->getCurMode()) {
    case LIST:
      return std::string("LIST");
    case SET:
      return std::string("SET");
    case HLL:
      return std::string("HLL");
    default:
      throw std::runtime_error("Sketch state error: Invalid hll_mode");
  }
}

template<typename A>
uint32_t hll_sketch_alloc<A>::get_max_updatable_serialization_bytes(uint8_t lg_config_k,
    const target_hll_type tgtHllType) {
  uint32_t arrBytes;
  if (tgtHllType == target_hll_type::HLL_4) {
    const uint32_t auxBytes = 4 << hll_constants::LG_AUX_ARR_INTS[lg_config_k];
    arrBytes = HllArray<A>::hll4ArrBytes(lg_config_k) + auxBytes;
  } else if (tgtHllType == target_hll_type::HLL_6) {
    arrBytes = HllArray<A>::hll6ArrBytes(lg_config_k);
  } else { //HLL_8
    arrBytes = HllArray<A>::hll8ArrBytes(lg_config_k);
  }
  return hll_constants::HLL_BYTE_ARR_START + arrBytes;
}

template<typename A>
double hll_sketch_alloc<A>::get_rel_err(bool upperBound, bool unioned,
                           uint8_t lg_config_k, uint8_t numStdDev) {
  return HllUtil<A>::getRelErr(upperBound, unioned, lg_config_k, numStdDev);
}

}

#endif // _HLLSKETCH_INTERNAL_HPP_
