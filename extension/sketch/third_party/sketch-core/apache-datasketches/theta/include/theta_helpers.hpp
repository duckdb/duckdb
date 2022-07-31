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

#ifndef THETA_HELPERS_HPP_
#define THETA_HELPERS_HPP_

#include <string>
#include <stdexcept>

namespace datasketches {

template<typename T>
static void check_value(T actual, T expected, const char* description) {
  if (actual != expected) {
    throw std::invalid_argument(std::string(description) + " mismatch: expected " + std::to_string(expected) + ", actual " + std::to_string(actual));
  }
}

template<bool dummy>
class checker {
public:
  static void check_serial_version(uint8_t actual, uint8_t expected) {
    check_value(actual, expected, "serial version");
  }
  static void check_sketch_family(uint8_t actual, uint8_t expected) {
    check_value(actual, expected, "sketch family");
  }
  static void check_sketch_type(uint8_t actual, uint8_t expected) {
    check_value(actual, expected, "sketch type");
  }
  static void check_seed_hash(uint16_t actual, uint16_t expected) {
    check_value(actual, expected, "seed hash");
  }
};

template<bool dummy>
class theta_build_helper{
public:
  // consistent way of initializing theta from p
  // avoids multiplication if p == 1 since it might not yield MAX_THETA exactly
  static uint64_t starting_theta_from_p(float p) {
    if (p < 1) return static_cast<uint64_t>(theta_constants::MAX_THETA * p);
    return theta_constants::MAX_THETA;
  }

  static uint8_t starting_sub_multiple(uint8_t lg_tgt, uint8_t lg_min, uint8_t lg_rf) {
    return (lg_tgt <= lg_min) ? lg_min : (lg_rf == 0) ? lg_tgt : ((lg_tgt - lg_min) % lg_rf) + lg_min;
  }
};

} /* namespace datasketches */

#endif
