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

// author Kevin Lang, Oath Research

#ifndef CPC_CONFIDENCE_HPP_
#define CPC_CONFIDENCE_HPP_

#include <cmath>
#include <stdexcept>

#include "cpc_sketch.hpp"

namespace datasketches {

// ln 2.0
static const double ICON_ERROT_CONSTANT = 0.693147180559945286;

//  1,    2,    3, // kappa
static const int16_t ICON_LOW_SIDE_DATA [33] = {   // Empirically measured at N = 1000 * K.
 6037, 5720, 5328, // 4 1000000
 6411, 6262, 5682, // 5 1000000
 6724, 6403, 6127, // 6 1000000
 6665, 6411, 6208, // 7 1000000
 6959, 6525, 6427, // 8 1000000
 6892, 6665, 6619, // 9 1000000
 6792, 6752, 6690, // 10 1000000
 6899, 6818, 6708, // 11 1000000
 6871, 6845, 6812, // 12 1046369
 6909, 6861, 6828, // 13 1043411
 6919, 6897, 6842, // 14 1000297
};                 // lgK numtrials

//  1,    2,    3, // kappa
static const int16_t ICON_HIGH_SIDE_DATA [33] = {   // Empirically measured at N = 1000 * K.
 8031, 8559, 9309, // 4 1000000
 7084, 7959, 8660, // 5 1000000
 7141, 7514, 7876, // 6 1000000
 7458, 7430, 7572, // 7 1000000
 6892, 7141, 7497, // 8 1000000
 6889, 7132, 7290, // 9 1000000
 7075, 7118, 7185, // 10 1000000
 7040, 7047, 7085, // 11 1000000
 6993, 7019, 7053, // 12 1046369
 6953, 7001, 6983, // 13 1043411
 6944, 6966, 7004, // 14 1000297
};                 // lgK numtrials

// sqrt((ln 2.0) / 2.0)
static const double HIP_ERROR_CONSTANT = 0.588705011257737332;

//  1,    2,    3, // kappa
static const int16_t HIP_LOW_SIDE_DATA [33] = {   // Empirically measured at N = 1000 * K.
 5871, 5247, 4826, // 4 1000000
 5877, 5403, 5070, // 5 1000000
 5873, 5533, 5304, // 6 1000000
 5878, 5632, 5464, // 7 1000000
 5874, 5690, 5564, // 8 1000000
 5880, 5745, 5619, // 9 1000000
 5875, 5784, 5701, // 10 1000000
 5866, 5789, 5742, // 11 1000000
 5869, 5827, 5784, // 12 1046369
 5876, 5860, 5827, // 13 1043411
 5881, 5853, 5842, // 14 1000297
};                 // lgK numtrials

//  1,    2,    3, // kappa
static const int16_t HIP_HIGH_SIDE_DATA [33] = {   // Empirically measured at N = 1000 * K.
 5855, 6688, 7391, // 4 1000000
 5886, 6444, 6923, // 5 1000000
 5885, 6254, 6594, // 6 1000000
 5889, 6134, 6326, // 7 1000000
 5900, 6072, 6203, // 8 1000000
 5875, 6005, 6089, // 9 1000000
 5871, 5980, 6040, // 10 1000000
 5889, 5941, 6015, // 11 1000000
 5871, 5926, 5973, // 12 1046369
 5866, 5901, 5915, // 13 1043411
 5880, 5914, 5953, // 14 1000297
};                 // lgK numtrials

template<typename A>
double get_icon_confidence_lb(const cpc_sketch_alloc<A>& sketch, int kappa) {
  if (sketch.get_num_coupons() == 0) return 0.0;
  const int lg_k = sketch.get_lg_k();
  const long k = 1 << lg_k;
  if (lg_k < 4) throw std::logic_error("lgk < 4");
  if (kappa < 1 || kappa > 3) throw std::invalid_argument("kappa must be between 1 and 3");
  double x = ICON_ERROT_CONSTANT;
  if (lg_k <= 14) x = ((double) ICON_HIGH_SIDE_DATA[3 * (lg_k - 4) + (kappa - 1)]) / 10000.0;
  const double rel = x / sqrt(k);
  const double eps = kappa * rel;
  const double est = sketch.get_icon_estimate();
  double result = est / (1.0 + eps);
  const double check = sketch.get_num_coupons();
  if (result < check) result = check;
  return result;
}

template<typename A>
double get_icon_confidence_ub(const cpc_sketch_alloc<A>& sketch, int kappa) {
  if (sketch.get_num_coupons() == 0) return 0.0;
  const int lg_k = sketch.get_lg_k();
  const long k = 1 << lg_k;
  if (lg_k < 4) throw std::logic_error("lgk < 4");
  if (kappa < 1 || kappa > 3) throw std::invalid_argument("kappa must be between 1 and 3");
  double x = ICON_ERROT_CONSTANT;
  if (lg_k <= 14) x = ((double) ICON_LOW_SIDE_DATA[3 * (lg_k - 4) + (kappa - 1)]) / 10000.0;
  const double rel = x / sqrt(k);
  const double eps = kappa * rel;
  const double est = sketch.get_icon_estimate();
  const double result = est / (1.0 - eps);
  return ceil(result); // widening for coverage
}

template<typename A>
double get_hip_confidence_lb(const cpc_sketch_alloc<A>& sketch, int kappa) {
  if (sketch.get_num_coupons() == 0) return 0.0;
  const int lg_k = sketch.get_lg_k();
  const long k = 1 << lg_k;
  if (lg_k < 4) throw std::logic_error("lgk < 4");
  if (kappa < 1 || kappa > 3) throw std::invalid_argument("kappa must be between 1 and 3");
  double x = HIP_ERROR_CONSTANT;
  if (lg_k <= 14) x = ((double) HIP_HIGH_SIDE_DATA[3 * (lg_k - 4) + (kappa - 1)]) / 10000.0;
  const double rel = x / (sqrt((double) k));
  const double eps = ((double) kappa) * rel;
  const double est = sketch.get_hip_estimate();
  double result = est / (1.0 + eps);
  const double check = (double) sketch.get_num_coupons();
  if (result < check) result = check;
  return result;
}

template<typename A>
double get_hip_confidence_ub(const cpc_sketch_alloc<A>& sketch, int kappa) {
  if (sketch.get_num_coupons() == 0) return 0.0;
  const int lg_k = sketch.get_lg_k();
  const long k = 1 << lg_k;
  if (lg_k < 4) throw std::logic_error("lgk < 4");
  if (kappa < 1 || kappa > 3) throw std::invalid_argument("kappa must be between 1 and 3");
  double x = HIP_ERROR_CONSTANT;
  if (lg_k <= 14) x = ((double) HIP_LOW_SIDE_DATA[3 * (lg_k - 4) + (kappa - 1)]) / 10000.0;
  const double rel = x / sqrt(k);
  const double eps = kappa * rel;
  const double est = sketch.get_hip_estimate();
  const double result = est / (1.0 - eps);
  return ceil(result); // widening for coverage
}

} /* namespace datasketches */

#endif
