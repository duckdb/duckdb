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

#ifndef _HLLUNION_INTERNAL_HPP_
#define _HLLUNION_INTERNAL_HPP_

#include "hll.hpp"

#include "HllSketchImpl.hpp"
#include "HllArray.hpp"
#include "HllUtil.hpp"

#include <stdexcept>
#include <string>

namespace datasketches {

template<typename A>
hll_union_alloc<A>::hll_union_alloc(uint8_t lg_max_k, const A& allocator):
  lg_max_k_(HllUtil<A>::checkLgK(lg_max_k)),
  gadget_(lg_max_k, target_hll_type::HLL_8, false, allocator)
{}

template<typename A>
hll_sketch_alloc<A> hll_union_alloc<A>::get_result(target_hll_type target_type) const {
  return hll_sketch_alloc<A>(gadget_, target_type);
}

template<typename A>
void hll_union_alloc<A>::update(const hll_sketch_alloc<A>& sketch) {
  if (sketch.is_empty()) return;
  union_impl(sketch, lg_max_k_);
}

template<typename A>
void hll_union_alloc<A>::update(hll_sketch_alloc<A>&& sketch) {
  if (sketch.is_empty()) return;
  if (gadget_.is_empty() && sketch.get_target_type() == HLL_8 && sketch.get_lg_config_k() <= lg_max_k_) {
    if (sketch.get_current_mode() == HLL || sketch.get_lg_config_k() == lg_max_k_) {
      gadget_ = std::move(sketch);
    }
  }
  union_impl(sketch, lg_max_k_);
}

template<typename A>
void hll_union_alloc<A>::update(const std::string& datum) {
  gadget_.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(uint64_t datum) {
  gadget_.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(uint32_t datum) {
  gadget_.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(uint16_t datum) {
  gadget_.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(uint8_t datum) {
  gadget_.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(int64_t datum) {
  gadget_.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(int32_t datum) {
  gadget_.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(int16_t datum) {
  gadget_.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(int8_t datum) {
  gadget_.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(double datum) {
  gadget_.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(float datum) {
  gadget_.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const void* data, size_t length_bytes) {
  gadget_.update(data, length_bytes);
}

template<typename A>
void hll_union_alloc<A>::coupon_update(uint32_t coupon) {
  if (coupon == HllUtil<A>::EMPTY) { return; }
  HllSketchImpl<A>* result = gadget_.sketch_impl->coupon_update(coupon);
  if (result != gadget_.sketch_impl) {
    if (gadget_.sketch_impl != nullptr) { gadget_.sketch_impl->get_deleter()(gadget_.sketch_impl); }
    gadget_.sketch_impl = result;
  }
}

template<typename A>
double hll_union_alloc<A>::get_estimate() const {
  return gadget_.get_estimate();
}

template<typename A>
double hll_union_alloc<A>::get_composite_estimate() const {
  return gadget_.get_composite_estimate();
}

template<typename A>
double hll_union_alloc<A>::get_lower_bound(uint8_t num_std_dev) const {
  return gadget_.get_lower_bound(num_std_dev);
}

template<typename A>
double hll_union_alloc<A>::get_upper_bound(uint8_t num_std_dev) const {
  return gadget_.get_upper_bound(num_std_dev);
}

template<typename A>
uint8_t hll_union_alloc<A>::get_lg_config_k() const {
  return gadget_.get_lg_config_k();
}

template<typename A>
void hll_union_alloc<A>::reset() {
  gadget_.reset();
}

template<typename A>
bool hll_union_alloc<A>::is_empty() const {
  return gadget_.is_empty();
}

template<typename A>
bool hll_union_alloc<A>::is_out_of_order_flag() const {
  return gadget_.is_out_of_order_flag();
}

template<typename A>
hll_mode hll_union_alloc<A>::get_current_mode() const {
  return gadget_.get_current_mode();
}

template<typename A>
bool hll_union_alloc<A>::is_estimation_mode() const {
  return gadget_.is_estimation_mode();
}

template<typename A>
target_hll_type hll_union_alloc<A>::get_target_type() const {
  return target_hll_type::HLL_8;
}

template<typename A>
double hll_union_alloc<A>::get_rel_err(bool upper_bound, bool unioned,
                           uint8_t lg_config_k, uint8_t num_std_dev) {
  return HllUtil<A>::getRelErr(upper_bound, unioned, lg_config_k, num_std_dev);
}

template<typename A>
HllSketchImpl<A>* hll_union_alloc<A>::copy_or_downsample(const HllSketchImpl<A>* src_impl, uint8_t tgt_lg_k) {
  if (src_impl->getCurMode() != HLL) {
    throw std::logic_error("Attempt to downsample non-HLL sketch");
  }
  const HllArray<A>* src = static_cast<const HllArray<A>*>(src_impl);
  const int src_lg_k = src->getLgConfigK();
  if (src_lg_k <= tgt_lg_k) {
    return src->copyAs(HLL_8);
  }
  typedef typename std::allocator_traits<A>::template rebind_alloc<Hll8Array<A>> hll8Alloc;
  Hll8Array<A>* tgtHllArr = new (hll8Alloc(src->getAllocator()).allocate(1)) Hll8Array<A>(tgt_lg_k, false, src->getAllocator());
  tgtHllArr->mergeHll(*src);
  //both of these are required for isomorphism
  tgtHllArr->putHipAccum(src->getHipAccum());
  tgtHllArr->putOutOfOrderFlag(src->isOutOfOrderFlag());
  return tgtHllArr;
}

template<typename A>
inline HllSketchImpl<A>* hll_union_alloc<A>::leak_free_coupon_update(HllSketchImpl<A>* impl, uint32_t coupon) {
  HllSketchImpl<A>* result = impl->couponUpdate(coupon);
  if (result != impl) {
    impl->get_deleter()(impl);
  }
  return result;
}

template<typename A>
void hll_union_alloc<A>::union_impl(const hll_sketch_alloc<A>& sketch, uint8_t lg_max_k) {
  const HllSketchImpl<A>* src_impl = sketch.sketch_impl; //default
  HllSketchImpl<A>* dst_impl = gadget_.sketch_impl; //default
  if (src_impl->getCurMode() == LIST || src_impl->getCurMode() == SET) {
    if (dst_impl->isEmpty() && src_impl->getLgConfigK() == dst_impl->getLgConfigK()) {
      dst_impl = src_impl->copyAs(HLL_8);
      gadget_.sketch_impl->get_deleter()(gadget_.sketch_impl); // gadget to be replaced
    } else {
      const CouponList<A>* src = static_cast<const CouponList<A>*>(src_impl);
      for (auto coupon: *src) {
        dst_impl = leak_free_coupon_update(dst_impl, coupon); //assignment required
      }
    }
  } else if (!dst_impl->isEmpty()) { // src is HLL
    if (dst_impl->getCurMode() == LIST || dst_impl->getCurMode() == SET) {
      // swap so that src is LIST or SET, tgt is HLL
      // use lg_max_k because LIST has effective K of 2^26
      const CouponList<A>* src = static_cast<const CouponList<A>*>(dst_impl);
      dst_impl = copy_or_downsample(src_impl, lg_max_k);
      static_cast<Hll8Array<A>*>(dst_impl)->mergeList(*src);
      gadget_.sketch_impl->get_deleter()(gadget_.sketch_impl); // gadget to be replaced
    } else { // gadget is HLL
      if (src_impl->getLgConfigK() < dst_impl->getLgConfigK()) {
        dst_impl = copy_or_downsample(dst_impl, sketch.get_lg_config_k());
        gadget_.sketch_impl->get_deleter()(gadget_.sketch_impl); // gadget to be replaced
      }
      const HllArray<A>* src = static_cast<const HllArray<A>*>(src_impl);
      static_cast<Hll8Array<A>*>(dst_impl)->mergeHll(*src);
      dst_impl->putOutOfOrderFlag(true);
      static_cast<Hll8Array<A>*>(dst_impl)->putHipAccum(0);
    }
  } else { // src is HLL, gadget is empty
    dst_impl = copy_or_downsample(src_impl, lg_max_k);
    gadget_.sketch_impl->get_deleter()(gadget_.sketch_impl); // gadget to be replaced
  }
  gadget_.sketch_impl = dst_impl; // gadget replaced
}

}

#endif // _HLLUNION_INTERNAL_HPP_
