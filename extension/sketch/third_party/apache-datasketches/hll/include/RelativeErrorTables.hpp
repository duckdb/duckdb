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

#ifndef _RELATIVEERRORTABLES_HPP_
#define _RELATIVEERRORTABLES_HPP_

#include <memory>

namespace datasketches {

template<typename A = std::allocator<uint8_t>>
class RelativeErrorTables {
  public:
    /**
     * Return Relative Error for UB or LB for HIP or Non-HIP as a function of numStdDev.
     * @param upperBound true if for upper bound
     * @param oooFlag true if for Non-HIP
     * @param lgK must be between 4 and 12 inclusive
     * @param stdDev must be between 1 and 3 inclusive
     * @return Relative Error for UB or LB for HIP or Non-HIP as a function of numStdDev.
     */
    static double getRelErr(bool upperBound, bool oooFlag,
                            int lgK, int stdDev);
};

}

#include "RelativeErrorTables-internal.hpp"

#endif /* _RELATIVEERRORTABLES_HPP_ */
