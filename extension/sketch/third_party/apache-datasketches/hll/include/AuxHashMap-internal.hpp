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

#ifndef _AUXHASHMAP_INTERNAL_HPP_
#define _AUXHASHMAP_INTERNAL_HPP_

#include <stdexcept>

#include "HllUtil.hpp"
#include "AuxHashMap.hpp"

namespace datasketches {

template<typename A>
AuxHashMap<A>::AuxHashMap(uint8_t lgAuxArrInts, uint8_t lgConfigK, const A& allocator):
lgConfigK(lgConfigK),
lgAuxArrInts(lgAuxArrInts),
auxCount(0),
entries(1ULL << lgAuxArrInts, 0, allocator)
{}

template<typename A>
AuxHashMap<A>* AuxHashMap<A>::newAuxHashMap(uint8_t lgAuxArrInts, uint8_t lgConfigK, const A& allocator) {
  return new (ahmAlloc(allocator).allocate(1)) AuxHashMap<A>(lgAuxArrInts, lgConfigK, allocator);
}

template<typename A>
AuxHashMap<A>* AuxHashMap<A>::newAuxHashMap(const AuxHashMap& that) {
  return new (ahmAlloc(that.entries.get_allocator()).allocate(1)) AuxHashMap<A>(that);
}

template<typename A>
AuxHashMap<A>* AuxHashMap<A>::deserialize(const void* bytes, size_t len,
                                          uint8_t lgConfigK,
                                          uint32_t auxCount, uint8_t lgAuxArrInts,
                                          bool srcCompact, const A& allocator) {
  uint8_t lgArrInts = lgAuxArrInts;
  if (srcCompact) { // early compact versions didn't use LgArr byte field so ignore input
    lgArrInts = HllUtil<A>::computeLgArrInts(HLL, auxCount, lgConfigK);
  } else { // updatable
    lgArrInts = lgAuxArrInts;
  }
  
  const uint32_t configKmask = (1 << lgConfigK) - 1;

  AuxHashMap<A>* auxHashMap;
  const uint32_t* auxPtr = static_cast<const uint32_t*>(bytes);
  if (srcCompact) {
    if (len < auxCount * sizeof(int)) {
      throw std::out_of_range("Input array too small to hold AuxHashMap image");
    }
    auxHashMap = new (ahmAlloc(allocator).allocate(1)) AuxHashMap<A>(lgArrInts, lgConfigK, allocator);
    for (uint32_t i = 0; i < auxCount; ++i) {
      const uint32_t pair = auxPtr[i];
      const uint32_t slotNo = HllUtil<A>::getLow26(pair) & configKmask;
      const uint8_t value = HllUtil<A>::getValue(pair);
      auxHashMap->mustAdd(slotNo, value);
    }
  } else { // updatable
    uint32_t itemsToRead = 1 << lgAuxArrInts;
    if (len < itemsToRead * sizeof(uint32_t)) {
      throw std::out_of_range("Input array too small to hold AuxHashMap image");
    }
    auxHashMap = new (ahmAlloc(allocator).allocate(1)) AuxHashMap<A>(lgArrInts, lgConfigK, allocator);
    for (uint32_t i = 0; i < itemsToRead; ++i) {
      const uint32_t pair = auxPtr[i];
      if (pair == hll_constants::EMPTY) { continue; }
      const uint32_t slotNo = HllUtil<A>::getLow26(pair) & configKmask;
      const uint8_t value = HllUtil<A>::getValue(pair);
      auxHashMap->mustAdd(slotNo, value);
    }
  }

  if (auxHashMap->getAuxCount() != auxCount) {
    make_deleter()(auxHashMap);
    throw std::invalid_argument("Deserialized AuxHashMap has wrong number of entries");
  }

  return auxHashMap;                                    
}

template<typename A>
AuxHashMap<A>* AuxHashMap<A>::deserialize(std::istream& is, uint8_t lgConfigK,
                                          uint32_t auxCount, uint8_t lgAuxArrInts,
                                          bool srcCompact, const A& allocator) {
  uint8_t lgArrInts = lgAuxArrInts;
  if (srcCompact) { // early compact versions didn't use LgArr byte field so ignore input
    lgArrInts = HllUtil<A>::computeLgArrInts(HLL, auxCount, lgConfigK);
  } else { // updatable
    lgArrInts = lgAuxArrInts;
  }

  AuxHashMap<A>* auxHashMap = new (ahmAlloc(allocator).allocate(1)) AuxHashMap<A>(lgArrInts, lgConfigK, allocator);
  typedef std::unique_ptr<AuxHashMap<A>, std::function<void(AuxHashMap<A>*)>> aux_hash_map_ptr;
  aux_hash_map_ptr aux_ptr(auxHashMap, auxHashMap->make_deleter());

  const uint32_t configKmask = (1 << lgConfigK) - 1;

  if (srcCompact) {
    for (uint32_t i = 0; i < auxCount; ++i) {
      const auto pair = read<int>(is);
      uint32_t slotNo = HllUtil<A>::getLow26(pair) & configKmask;
      uint8_t value = HllUtil<A>::getValue(pair);
      auxHashMap->mustAdd(slotNo, value);
    }
  } else { // updatable
    const uint32_t itemsToRead = 1 << lgAuxArrInts;
    for (uint32_t i = 0; i < itemsToRead; ++i) {
      const auto pair = read<int>(is);
      if (pair == hll_constants::EMPTY) { continue; }
      const uint32_t slotNo = HllUtil<A>::getLow26(pair) & configKmask;
      const uint8_t value = HllUtil<A>::getValue(pair);
      auxHashMap->mustAdd(slotNo, value);
    }
  }

  if (auxHashMap->getAuxCount() != auxCount) {
    make_deleter()(auxHashMap);
    throw std::invalid_argument("Deserialized AuxHashMap has wrong number of entries");
  }

  return aux_ptr.release();
}

template<typename A>
std::function<void(AuxHashMap<A>*)> AuxHashMap<A>::make_deleter() {
  return [](AuxHashMap<A>* ptr) {
    ahmAlloc alloc(ptr->entries.get_allocator());
    ptr->~AuxHashMap();
    alloc.deallocate(ptr, 1);
  };
}

template<typename A>
AuxHashMap<A>* AuxHashMap<A>::copy() const {
  return new (ahmAlloc(entries.get_allocator()).allocate(1)) AuxHashMap<A>(*this);
}

template<typename A>
uint32_t AuxHashMap<A>::getAuxCount() const {
  return auxCount;
}

template<typename A>
uint32_t* AuxHashMap<A>::getAuxIntArr(){
  return entries.data();
}

template<typename A>
uint8_t AuxHashMap<A>::getLgAuxArrInts() const {
  return lgAuxArrInts;
}

template<typename A>
uint32_t AuxHashMap<A>::getCompactSizeBytes() const {
  return auxCount << 2;
}

template<typename A>
uint32_t AuxHashMap<A>::getUpdatableSizeBytes() const {
  return 4 << lgAuxArrInts;
}

template<typename A>
void AuxHashMap<A>::mustAdd(uint32_t slotNo, uint8_t value) {
  const int32_t index = find(entries.data(), lgAuxArrInts, lgConfigK, slotNo);
  const uint32_t entry_pair = HllUtil<A>::pair(slotNo, value);
  if (index >= 0) {
    throw std::invalid_argument("Found a slotNo that should not be there: SlotNo: "
                                + std::to_string(slotNo) + ", Value: " + std::to_string(value));
  }

  // found empty entry
  entries[~index] = entry_pair;
  ++auxCount;
  checkGrow();
}

template<typename A>
uint8_t AuxHashMap<A>::mustFindValueFor(uint32_t slotNo) const {
  const int32_t index = find(entries.data(), lgAuxArrInts, lgConfigK, slotNo);
  if (index >= 0) {
    return HllUtil<A>::getValue(entries[index]);
  }

  throw std::invalid_argument("slotNo not found: " + std::to_string(slotNo));
}

template<typename A>
void AuxHashMap<A>::mustReplace(uint32_t slotNo, uint8_t value) {
  const int32_t idx = find(entries.data(), lgAuxArrInts, lgConfigK, slotNo);
  if (idx >= 0) {
    entries[idx] = HllUtil<A>::pair(slotNo, value);
    return;
  }

  throw std::invalid_argument("Pair not found: SlotNo: " + std::to_string(slotNo)
                              + ", Value: " + std::to_string(value));
}

template<typename A>
void AuxHashMap<A>::checkGrow() {
  if ((hll_constants::RESIZE_DENOM * auxCount) > (hll_constants::RESIZE_NUMER * (1 << lgAuxArrInts))) {
    growAuxSpace();
  }
}

template<typename A>
void AuxHashMap<A>::growAuxSpace() {
  const int configKmask = (1 << lgConfigK) - 1;
  const int newArrLen = 1 << ++lgAuxArrInts;
  vector_int entries_new(newArrLen, 0, entries.get_allocator());
  for (size_t i = 0; i < entries.size(); ++i) {
    const uint32_t fetched = entries[i];
    if (fetched != hll_constants::EMPTY) {
      // find empty in new array
      const int32_t idx = find(entries_new.data(), lgAuxArrInts, lgConfigK, fetched & configKmask);
      entries_new[~idx] = fetched;
    }
  }
  entries = std::move(entries_new);
}

//Searches the Aux arr hash table for an empty or a matching slotNo depending on the context.
//If entire entry is empty, returns one's complement of index = found empty.
//If entry contains given slotNo, returns its index = found slotNo.
//Continues searching.
//If the probe comes back to original index, throws an exception.
template<typename A>
int32_t AuxHashMap<A>::find(const uint32_t* auxArr, uint8_t lgAuxArrInts, uint8_t lgConfigK, uint32_t slotNo) {
  const uint32_t auxArrMask = (1 << lgAuxArrInts) - 1;
  const uint32_t configKmask = (1 << lgConfigK) - 1;
  uint32_t probe = slotNo & auxArrMask;
  const uint32_t loopIndex = probe;
  do {
    const uint32_t arrVal = auxArr[probe];
    if (arrVal == hll_constants::EMPTY) { //Compares on entire entry
      return ~probe; //empty
    }
    else if (slotNo == (arrVal & configKmask)) { //Compares only on slotNo
      return probe; //found given slotNo, return probe = index into aux array
    }
    const uint32_t stride = (slotNo >> lgAuxArrInts) | 1;
    probe = (probe + stride) & auxArrMask;
  } while (probe != loopIndex);
  throw std::runtime_error("Key not found and no empty slots!");
}

template<typename A>
coupon_iterator<A> AuxHashMap<A>::begin(bool all) const {
  return coupon_iterator<A>(entries.data(), 1ULL << lgAuxArrInts, 0, all);
}

template<typename A>
coupon_iterator<A> AuxHashMap<A>::end() const {
  return coupon_iterator<A>(entries.data(), 1ULL << lgAuxArrInts, 1ULL << lgAuxArrInts, false);
}

}

#endif // _AUXHASHMAP_INTERNAL_HPP_
