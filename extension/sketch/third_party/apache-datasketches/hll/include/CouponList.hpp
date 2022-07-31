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

#ifndef _COUPONLIST_HPP_
#define _COUPONLIST_HPP_

#include "HllSketchImpl.hpp"
#include "coupon_iterator.hpp"

#include <iostream>

namespace datasketches {

template<typename A>
class HllSketchImplFactory;

template<typename A>
class CouponList : public HllSketchImpl<A> {
  public:
    CouponList(uint8_t lgConfigK, target_hll_type tgtHllType, hll_mode mode, const A& allocator);
    CouponList(const CouponList& that, target_hll_type tgtHllType);

    static CouponList* newList(const void* bytes, size_t len, const A& allocator);
    static CouponList* newList(std::istream& is, const A& allocator);
    virtual vector_u8<A> serialize(bool compact, unsigned header_size_bytes) const;
    virtual void serialize(std::ostream& os, bool compact) const;

    virtual ~CouponList() = default;
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

    virtual CouponList* copy() const;
    virtual CouponList* copyAs(target_hll_type tgtHllType) const;

    virtual HllSketchImpl<A>* couponUpdate(uint32_t coupon);

    virtual double getEstimate() const;
    virtual double getCompositeEstimate() const;
    virtual double getUpperBound(uint8_t numStdDev) const;
    virtual double getLowerBound(uint8_t numStdDev) const;

    virtual bool isEmpty() const;
    virtual uint32_t getCouponCount() const;

    coupon_iterator<A> begin(bool all = false) const;
    coupon_iterator<A> end() const;

  protected:
    using ClAlloc = typename std::allocator_traits<A>::template rebind_alloc<CouponList<A>>;

    using vector_int = std::vector<uint32_t, typename std::allocator_traits<A>::template rebind_alloc<uint32_t>>;

    HllSketchImpl<A>* promoteHeapListToSet(CouponList& list);
    HllSketchImpl<A>* promoteHeapListOrSetToHll(CouponList& src);

    virtual uint32_t getUpdatableSerializationBytes() const;
    virtual uint32_t getCompactSerializationBytes() const;
    virtual uint32_t getMemDataStart() const;
    virtual uint8_t getPreInts() const;
    virtual bool isCompact() const;
    virtual bool isOutOfOrderFlag() const;
    virtual void putOutOfOrderFlag(bool oooFlag);

    virtual A getAllocator() const;

    uint32_t couponCount_;
    bool oooFlag_;
    vector_int coupons_;

    friend class HllSketchImplFactory<A>;
};

}

#endif /* _COUPONLIST_HPP_ */
