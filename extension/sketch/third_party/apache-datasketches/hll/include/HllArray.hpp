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

#ifndef _HLLARRAY_HPP_
#define _HLLARRAY_HPP_

#include "HllSketchImpl.hpp"
#include "HllUtil.hpp"

namespace datasketches {

template<typename A>
class AuxHashMap;

template<typename A>
class HllArray : public HllSketchImpl<A> {
  public:
    HllArray(uint8_t lgConfigK, target_hll_type tgtHllType, bool startFullSize, const A& allocator);

    static HllArray* newHll(const void* bytes, size_t len, const A& allocator);
    static HllArray* newHll(std::istream& is, const A& allocator);

    virtual vector_u8<A> serialize(bool compact, unsigned header_size_bytes) const;
    virtual void serialize(std::ostream& os, bool compact) const;

    virtual ~HllArray() = default;
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const = 0;

    virtual HllArray* copy() const = 0;
    virtual HllArray* copyAs(target_hll_type tgtHllType) const;

    virtual HllSketchImpl<A>* couponUpdate(uint32_t coupon) = 0;

    virtual double getEstimate() const;
    virtual double getCompositeEstimate() const;
    virtual double getLowerBound(uint8_t numStdDev) const;
    virtual double getUpperBound(uint8_t numStdDev) const;

    inline void addToHipAccum(double delta);

    inline void decNumAtCurMin();

    inline uint8_t getCurMin() const;
    inline uint32_t getNumAtCurMin() const;
    inline double getHipAccum() const;

    virtual uint32_t getHllByteArrBytes() const = 0;

    virtual uint32_t getUpdatableSerializationBytes() const;
    virtual uint32_t getCompactSerializationBytes() const;

    virtual bool isOutOfOrderFlag() const;
    virtual bool isEmpty() const;
    virtual bool isCompact() const;

    virtual void putOutOfOrderFlag(bool flag);

    inline double getKxQ0() const;
    inline double getKxQ1() const;

    virtual uint32_t getMemDataStart() const;
    virtual uint8_t getPreInts() const;

    void putCurMin(uint8_t curMin);
    void putHipAccum(double hipAccum);
    inline void putKxQ0(double kxq0);
    inline void putKxQ1(double kxq1);
    void putNumAtCurMin(uint32_t numAtCurMin);

    static uint32_t hllArrBytes(target_hll_type tgtHllType, uint8_t lgConfigK);
    static uint32_t hll4ArrBytes(uint8_t lgConfigK);
    static uint32_t hll6ArrBytes(uint8_t lgConfigK);
    static uint32_t hll8ArrBytes(uint8_t lgConfigK);

    virtual AuxHashMap<A>* getAuxHashMap() const;

    class const_iterator;
    virtual const_iterator begin(bool all = false) const;
    virtual const_iterator end() const;

    virtual A getAllocator() const;

  protected:
    void hipAndKxQIncrementalUpdate(uint8_t oldValue, uint8_t newValue);
    double getHllBitMapEstimate() const;
    double getHllRawEstimate() const;

    double hipAccum_;
    double kxq0_;
    double kxq1_;
    vector_u8<A> hllByteArr_; //init by sub-classes
    uint8_t curMin_; //always zero for Hll6 and Hll8, only tracked by Hll4Array
    uint32_t numAtCurMin_; //interpreted as num zeros when curMin == 0
    bool oooFlag_; //Out-Of-Order Flag

    friend class HllSketchImplFactory<A>;
};

template<typename A>
class HllArray<A>::const_iterator: public std::iterator<std::input_iterator_tag, uint32_t> {
public:
  const_iterator(const uint8_t* array, uint32_t array_slze, uint32_t index, target_hll_type hll_type, const AuxHashMap<A>* exceptions, uint8_t offset, bool all);
  const_iterator& operator++();
  bool operator!=(const const_iterator& other) const;
  uint32_t operator*() const;
private:
  const uint8_t* array_;
  uint32_t array_size_;
  uint32_t index_;
  target_hll_type hll_type_;
  const AuxHashMap<A>* exceptions_;
  uint8_t offset_;
  bool all_;
  uint8_t value_; // cached value to avoid computing in operator++ and in operator*()
  static inline uint8_t get_value(const uint8_t* array, uint32_t index, target_hll_type hll_type, const AuxHashMap<A>* exceptions, uint8_t offset);
};

}

#endif /* _HLLARRAY_HPP_ */
