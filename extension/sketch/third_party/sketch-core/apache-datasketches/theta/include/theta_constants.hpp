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

#ifndef THETA_CONSTANTS_HPP_
#define THETA_CONSTANTS_HPP_

#include <climits>
#include "common_defs.hpp"

namespace datasketches {

namespace theta_constants {
  using resize_factor = datasketches::resize_factor;
  //enum resize_factor { X1, X2, X4, X8 };
  const uint64_t MAX_THETA = LLONG_MAX; // signed max for compatibility with Java
  const uint8_t MIN_LG_K = 5;
  const uint8_t MAX_LG_K = 26;

  const uint8_t DEFAULT_LG_K = 12;
  const resize_factor DEFAULT_RESIZE_FACTOR = resize_factor::X8;
}

} /* namespace datasketches */

#endif
