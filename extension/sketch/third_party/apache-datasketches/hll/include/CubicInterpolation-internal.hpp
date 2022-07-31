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

#ifndef _CUBICINTERPOLATION_INTERNAL_HPP_
#define _CUBICINTERPOLATION_INTERNAL_HPP_

#include "CubicInterpolation.hpp"

#include <string>
#include <stdexcept>

namespace datasketches {

template<typename A>
static double interpolateUsingXAndYTables(const double xArr[], const double yArr[], const int offset, const double x);

template<typename A>
static double cubicInterpolate(const double x0, const double y0, const double x1, const double y1,
                               const double x2, const double y2, const double x3, const double y3, const double x);

template<typename A>
static int findStraddle(const double xArr[], const int len, const double x);

template<typename A>
static int recursiveFindStraddle(const double xArr[], const int l, const int r, const double x);

template<typename A>
static double interpolateUsingXArrAndYStride(const double xArr[], const double yStride,
                                             const int offset, const double x);

const int numEntries = 40;

//Computed for Coupon lgK = 26 ONLY. Designed for the cubic interpolator function.
const double xArrComputed[numEntries] = {
    0.0, 1.0, 20.0, 400.0,
    8000.0, 160000.0, 300000.0, 600000.0,
    900000.0, 1200000.0, 1500000.0, 1800000.0,
    2100000.0, 2400000.0, 2700000.0, 3000000.0,
    3300000.0, 3600000.0, 3900000.0, 4200000.0,
    4500000.0, 4800000.0, 5100000.0, 5400000.0,
    5700000.0, 6000000.0, 6300000.0, 6600000.0,
    6900000.0, 7200000.0, 7500000.0, 7800000.0,
    8100000.0, 8400000.0, 8700000.0, 9000000.0,
    9300000.0, 9600000.0, 9900000.0, 10200000.0
};

//Computed for Coupon lgK = 26 ONLY. Designed for the cubic interpolator function.
const double yArrComputed[numEntries] =  {
    0.0000000000000000, 1.0000000000000000, 20.0000009437402611, 400.0003963713384110,
    8000.1589294602090376, 160063.6067763759638183, 300223.7071597663452849, 600895.5933856170158833,
    902016.8065120954997838, 1203588.4983199508860707, 1505611.8245524743106216, 1808087.9449319066479802,
    2111018.0231759352609515, 2414403.2270142501220107, 2718244.7282051891088486, 3022543.7025524540804327,
    3327301.3299219091422856, 3632518.7942584538832307, 3938197.2836029687896371, 4244337.9901093561202288,
    4550942.1100616492331028, 4858010.8438911894336343, 5165545.3961938973516226, 5473546.9757476449012756,
    5782016.7955296505242586, 6090956.0727340159937739, 6400366.0287892958149314, 6710247.8893762007355690,
    7020602.8844453142955899, 7331432.2482349723577499, 7642737.2192891482263803, 7954519.0404754765331745,
    8266778.9590033423155546, 8579518.2264420464634895, 8892738.0987390466034412, 9206439.8362383283674717,
    9520624.7036988288164139, 9835293.9703129194676876, 10150448.9097250290215015, 10466090.8000503256917000
};

template<typename A>
double CubicInterpolation<A>::usingXAndYTables(const double x) {
  return usingXAndYTables(xArrComputed, yArrComputed, numEntries, x);
}

template<typename A>
double CubicInterpolation<A>::usingXAndYTables(const double xArr[], const double yArr[],
                                            const int len, const double x) {
  int offset;
  if (x < xArr[0] || x > xArr[len-1]) {
    throw std::invalid_argument("x value out of range: " + std::to_string(x));
  }

  if (x ==  xArr[len-1]) { // corner case
    return (yArr[len-1]);
  }

  offset = findStraddle<A>(xArr, len, x);
  if (offset < 0 && offset > len-2) {
    throw std::logic_error("offset must be >= 0 and <= " + std::to_string(len) + "-2");
  }

  if (offset == 0) { // corner case
    return (interpolateUsingXAndYTables<A>(xArr, yArr, (offset-0), x));
  }
  else if (offset == numEntries-2) { // corner case
    return (interpolateUsingXAndYTables<A>(xArr, yArr, (offset-2), x));
  }
  // main case
  return (interpolateUsingXAndYTables<A>(xArr, yArr, (offset-1), x));
}

// In C: again-two-registers cubic_interpolate_aux L1368
template<typename A>
static double interpolateUsingXAndYTables(const double xArr[], const double yArr[],
                                          const int offset, const double x) {
    return (cubicInterpolate<A>(xArr[offset+0], yArr[offset+0],
                        xArr[offset+1], yArr[offset+1],
                        xArr[offset+2], yArr[offset+2],
                        xArr[offset+3], yArr[offset+3],
                        x) );
}

template<typename A>
static inline double cubicInterpolate(const double x0, const double y0,
                                      const double x1, const double y1,
                                      const double x2, const double y2,
                                      const double x3, const double y3,
                                      const double x)
{
  double l0_numer = (x - x1) * (x - x2) * (x - x3);
  double l1_numer = (x - x0) * (x - x2) * (x - x3);
  double l2_numer = (x - x0) * (x - x1) * (x - x3);
  double l3_numer = (x - x0) * (x - x1) * (x - x2);

  double l0_denom = (x0 - x1) * (x0 - x2) * (x0 - x3);
  double l1_denom = (x1 - x0) * (x1 - x2) * (x1 - x3);
  double l2_denom = (x2 - x0) * (x2 - x1) * (x2 - x3);
  double l3_denom = (x3 - x0) * (x3 - x1) * (x3 - x2);

  double term0 = y0 * l0_numer / l0_denom;
  double term1 = y1 * l1_numer / l1_denom;
  double term2 = y2 * l2_numer / l2_denom;
  double term3 = y3 * l3_numer / l3_denom;

  return (term0 + term1 + term2 + term3);
}

/* returns j such that xArr[j] <= x and x < xArr[j+1] */
template<typename A>
static int findStraddle(const double xArr[], const int len, const double x)
{
  if ((len < 2) || (x < xArr[0]) || (x > xArr[len-1])) {
    throw std::logic_error("invariant violated during interpolation");
  }
  return(recursiveFindStraddle<A>(xArr, 0, len-1, x));
}


/* the invariant here is that xArr[l] <= x && x < xArr[r] */
template<typename A>
static int recursiveFindStraddle(const double xArr[], const int l, const int r, const double x)
{
  int m;
  if (l >= r) {
    throw std::logic_error("lower bound not less than upper bound in search");
  }
  if ((xArr[l] > x) || (x >= xArr[r])) { // the invariant
    throw std::logic_error("target value invariant violated in search");
  }

  if (l+1 == r) return (l);
  m = l + ((r-l)/2);
  if (xArr[m] <= x) return (recursiveFindStraddle<A>(xArr, m, r, x));
  else              return (recursiveFindStraddle<A>(xArr, l, m, x));
}


//Interpolate using X table and Y stride

/**
 * Cubic interpolation using interpolation X table and Y stride.
 *
 * @param xArr The x array
 * @param yStride The y stride
 * @param xArrLen the length of xArr
 * @param x The value x
 * @return cubic interpolation
 */
//In C: again-two-registers cubic_interpolate_with_x_arr_and_y_stride L1411
// Used by HllEstimators
template<typename A>
double CubicInterpolation<A>::usingXArrAndYStride(const double xArr[], const int xArrLen,
                                                  const double yStride, const double x) {
  const int xArrLenM1 = xArrLen - 1;

  if ((xArrLen < 4) || (x < xArr[0]) || (x > xArr[xArrLenM1])) {
    throw std::logic_error("impossible values during interpolaiton");
  }

  if (x ==  xArr[xArrLenM1]) { /* corner case */
    return (yStride * (xArrLenM1));
  }

  const int offset = findStraddle<A>(xArr, xArrLen, x); //uses recursion
  const int xArrLenM2 = xArrLen - 2;
  if ((offset < 0) || (offset > xArrLenM2)) {
    throw std::logic_error("invalid offset during interpolation");
  }

  if (offset == 0) { /* corner case */
    return (interpolateUsingXArrAndYStride<A>(xArr, yStride, (offset - 0), x));
  }
  else if (offset == xArrLenM2) { /* corner case */
    return (interpolateUsingXArrAndYStride<A>(xArr, yStride, (offset - 2), x));
  }
  /* main case */
  return (interpolateUsingXArrAndYStride<A>(xArr, yStride, (offset - 1), x));
}

//In C: again-two-registers cubic_interpolate_with_x_arr_and_y_stride_aux L1402
template<typename A>
static double interpolateUsingXArrAndYStride(const double xArr[], const double yStride,
                                             const int offset, const double x) {
  return cubicInterpolate<A>(
      xArr[offset + 0], yStride * (offset + 0),
      xArr[offset + 1], yStride * (offset + 1),
      xArr[offset + 2], yStride * (offset + 2),
      xArr[offset + 3], yStride * (offset + 3),
      x);
}

}

#endif // _CUBICINTERPOLATION_INTERNAL_HPP_
