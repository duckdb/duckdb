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

#ifndef _RELATIVEERRORTABLES_INTERNAL_HPP_
#define _RELATIVEERRORTABLES_INTERNAL_HPP_

#include "RelativeErrorTables.hpp"

namespace datasketches {

//case 0
static double HIP_LB[] = //sd 1, 2, 3
  { //Q(.84134), Q(.97725), Q(.99865) respectively
    0.207316195, 0.502865572, 0.882303765, //4
    0.146981579, 0.335426881, 0.557052,    //5
    0.104026721, 0.227683872, 0.365888317, //6
    0.073614601, 0.156781585, 0.245740374, //7
    0.05205248,  0.108783763, 0.168030442, //8
    0.036770852, 0.075727545, 0.11593785,  //9
    0.025990219, 0.053145536, 0.080772263, //10
    0.018373987, 0.037266176, 0.056271814, //11
    0.012936253, 0.02613829,  0.039387631, //12
  };

//case 1
static double HIP_UB[] = //sd 1, 2, 3
  { //Q(.15866), Q(.02275), Q(.00135) respectively
    -0.207805347, -0.355574279, -0.475535095, //4
    -0.146988328, -0.262390832, -0.360864026, //5
    -0.103877775, -0.191503663, -0.269311582, //6
    -0.073452978, -0.138513438, -0.198487447, //7
    -0.051982806, -0.099703123, -0.144128618, //8
    -0.036768609, -0.07138158,  -0.104430324, //9
    -0.025991325, -0.050854296, -0.0748143,   //10
    -0.01834533,  -0.036121138, -0.05327616,  //11
    -0.012920332, -0.025572893, -0.037896952, //12
  };

//case 2
static double NON_HIP_LB[] = //sd 1, 2, 3`
  { //Q(.84134), Q(.97725), Q(.99865) respectively
    0.254409839, 0.682266712, 1.304022158, //4
    0.181817353, 0.443389054, 0.778776219, //5
    0.129432281, 0.295782195, 0.49252279,  //6
    0.091640655, 0.201175925, 0.323664385, //7
    0.064858051, 0.138523393, 0.218805328, //8
    0.045851855, 0.095925072, 0.148635751, //9
    0.032454144, 0.067009668, 0.102660669, //10
    0.022921382, 0.046868565, 0.071307398, //11
    0.016155679, 0.032825719, 0.049677541  //12
  };

//case 3
static double NON_HIP_UB[] = //sd 1, 2, 3
  { //Q(.15866), Q(.02275), Q(.00135) respectively
    -0.256980172, -0.411905944, -0.52651057,  //4
    -0.182332109, -0.310275547, -0.412660505, //5
    -0.129314228, -0.230142294, -0.315636197, //6
    -0.091584836, -0.16834013,  -0.236346847, //7
    -0.06487411,  -0.122045231, -0.174112107, //8
    -0.04591465,  -0.08784505,  -0.126917615, //9
    -0.032433119, -0.062897613, -0.091862929, //10
    -0.022960633, -0.044875401, -0.065736049, //11
    -0.016186662, -0.031827816, -0.046973459  //12
  };

template<typename A>
double RelativeErrorTables<A>::getRelErr(const bool upperBound, const bool oooFlag,
                                         const int lgK, const int stdDev) {
  const int idx = ((lgK - 4) * 3) + (stdDev - 1);
  const int sw = (oooFlag ? 2 : 0) | (upperBound ? 1 : 0);
  double f = 0;
  switch (sw) {
    case 0 : { // HIP, LB
      f = HIP_LB[idx];
      break;
    }
    case 1 : { // HIP, UB
      f = HIP_UB[idx];
      break;
    }
    case 2 : { // NON_HIP, LB
      f = NON_HIP_LB[idx];
      break;
    }
    case 3 : { // NON_HIP, UB
      f = NON_HIP_UB[idx];
      break;
    }
  }
  return f;
}

}

#endif // _RELATIVEERRORTABLES_INTERNAL_HPP_