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

#ifndef _COUPONHASHSET_HPP_
#define _COUPONHASHSET_HPP_

#include "CouponList.hpp"

namespace datasketches {

template<typename A>
class CouponHashSet : public CouponList<A> {
  public:
    static CouponHashSet* newSet(const void* bytes, size_t len, const A& allocator);
    static CouponHashSet* newSet(std::istream& is, const A& allocator);
    CouponHashSet(uint8_t lgConfigK, target_hll_type tgtHllType, const A& allocator);
    CouponHashSet(const CouponHashSet& that, target_hll_type tgtHllType);

    virtual ~CouponHashSet() = default;
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

  protected:
    using vector_int = std::vector<uint32_t, typename std::allocator_traits<A>::template rebind_alloc<uint32_t>>;

    virtual CouponHashSet* copy() const;
    virtual CouponHashSet* copyAs(target_hll_type tgtHllType) const;

    virtual HllSketchImpl<A>* couponUpdate(uint32_t coupon);

    virtual uint32_t getMemDataStart() const;
    virtual uint8_t getPreInts() const;

    friend class HllSketchImplFactory<A>;

  private:
    using ChsAlloc = typename std::allocator_traits<A>::template rebind_alloc<CouponHashSet<A>>;
    bool checkGrowOrPromote();
    void growHashSet(uint8_t tgtLgCoupArrSize);
};

}

#endif /* _COUPONHASHSET_HPP_ */
