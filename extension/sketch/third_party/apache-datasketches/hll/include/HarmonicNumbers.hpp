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

#ifndef _HARMONICNUMBERS_HPP_
#define _HARMONICNUMBERS_HPP_

#include <cstdint>
#include <memory>

namespace datasketches {

template<typename A = std::allocator<uint8_t>>
class HarmonicNumbers {
  public:
    /**
     * This is the estimator you would use for flat bit map random accessed, similar to a Bloom filter.
     * @param bitVectorLength the length of the bit vector in bits. Must be &gt; 0.
     * @param numBitsSet the number of bits set in this bit vector. Must be &ge; 0 and &le;
     * bitVectorLength.
     * @return the estimate.
     */
    static double getBitMapEstimate(int bitVectorLength, int numBitsSet);

  private:
    static double harmonicNumber(uint64_t x_i);
};

}

#include "HarmonicNumbers-internal.hpp"

#endif /* _HARMONICNUMBERS_HPP_ */
