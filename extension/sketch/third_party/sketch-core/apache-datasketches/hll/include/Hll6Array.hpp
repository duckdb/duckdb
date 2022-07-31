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

#ifndef _HLL6ARRAY_HPP_
#define _HLL6ARRAY_HPP_

#include "HllArray.hpp"

namespace datasketches {

template<typename A>
class Hll6Iterator;

template<typename A>
class Hll6Array final : public HllArray<A> {
  public:
    Hll6Array(uint8_t lgConfigK, bool startFullSize, const A& allocator);

    virtual ~Hll6Array() = default;
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

    virtual Hll6Array* copy() const;

    inline uint8_t getSlot(uint32_t slotNo) const;
    inline void putSlot(uint32_t slotNo, uint8_t value);

    virtual HllSketchImpl<A>* couponUpdate(uint32_t coupon) final;
    void mergeHll(const HllArray<A>& src);

    virtual uint32_t getHllByteArrBytes() const;

  private:
    void internalCouponUpdate(uint32_t coupon);
};

}

#endif /* _HLL6ARRAY_HPP_ */
