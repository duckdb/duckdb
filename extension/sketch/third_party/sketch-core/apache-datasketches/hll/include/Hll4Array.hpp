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

#ifndef _HLL4ARRAY_HPP_
#define _HLL4ARRAY_HPP_

#include "AuxHashMap.hpp"
#include "HllArray.hpp"

namespace datasketches {

template<typename A>
class Hll4Iterator;

template<typename A>
class Hll4Array final : public HllArray<A> {
  public:
    explicit Hll4Array(uint8_t lgConfigK, bool startFullSize, const A& allocator);
    explicit Hll4Array(const Hll4Array<A>& that);

    virtual ~Hll4Array();
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

    virtual Hll4Array* copy() const;

    inline uint8_t getSlot(uint32_t slotNo) const;
    inline void putSlot(uint32_t slotNo, uint8_t value);
    inline uint8_t get_value(uint32_t index) const;

    virtual uint32_t getUpdatableSerializationBytes() const;
    virtual uint32_t getHllByteArrBytes() const;

    virtual HllSketchImpl<A>* couponUpdate(uint32_t coupon) final;
    void mergeHll(const HllArray<A>& src);

    virtual AuxHashMap<A>* getAuxHashMap() const;
    // does *not* delete old map if overwriting
    void putAuxHashMap(AuxHashMap<A>* auxHashMap);

    virtual typename HllArray<A>::const_iterator begin(bool all = false) const;
    virtual typename HllArray<A>::const_iterator end() const;

  private:
    void internalCouponUpdate(uint32_t coupon);
    void internalHll4Update(uint32_t slotNo, uint8_t newVal);
    void shiftToBiggerCurMin();

    AuxHashMap<A>* auxHashMap_;
};

}

#endif /* _HLL4ARRAY_HPP_ */
