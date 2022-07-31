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

#include <sstream>
#include <stdexcept>

#include <catch.hpp>
#include <theta_union.hpp>
#include <theta_intersection.hpp>
#include <theta_a_not_b.hpp>

namespace datasketches {

static const uint64_t GT_MIDP_V  = 3L;
static const float MIDP          = 0.5f;

static const uint64_t GT_LOWP_V  = 6L;
static const float LOWP          = 0.1f;
static const long LT_LOWP_V      = 4L;

static const double LOWP_THETA   = LOWP;

enum SkType {
  EMPTY,      // { 1.0,  0, T} Bin: 101  Oct: 05
  EXACT,      // { 1.0, >0, F} Bin: 110  Oct: 06, specify only value
  ESTIMATION, // {<1.0, >0, F} Bin: 010  Oct: 02, specify only value
  DEGENERATE  // {<1.0,  0, F} Bin: 000  Oct: 00, specify p, value
};

void check_result(std::string comment, compact_theta_sketch sk, double theta, uint32_t entries, bool empty) {
  bool thetaOk = sk.get_theta() == theta;
  bool entriesOk = sk.get_num_retained() == entries;
  bool emptyOk = sk.is_empty() == empty;
  if (!thetaOk || !entriesOk || !emptyOk) {
    std::ostringstream s;
    s << comment << ": ";
    if (!thetaOk)   { s << "theta: expected " << theta << ", got " << sk.get_theta() << "; "; }
    if (!entriesOk) { s << "entries: expected " << entries << ", got " << sk.get_num_retained() << "; "; }
    if (!emptyOk)   { s << "empty: expected " << empty << ", got " << sk.is_empty() << "."; }
    FAIL(s.str());
  }
}

update_theta_sketch build_sketch(SkType skType, float p, uint64_t value) {
  update_theta_sketch::builder bldr;
  bldr.set_lg_k(5);

  switch(skType) {
    case EMPTY: { // { 1.0,  0, T}
      return bldr.build();
    }
    case EXACT: { // { 1.0, >0, F}
      auto sk = bldr.build();
      sk.update(value);
      return sk;
    }
    case ESTIMATION: { // {<1.0, >0, F}
      bldr.set_p(p);
      auto sk = bldr.build();
      sk.update(value);
      return sk;
    }
    case DEGENERATE: { // {<1.0,  0, F}
      bldr.set_p(p);
      auto sk = bldr.build();
      sk.update(value); // > theta
      return sk;
    }
    default: throw std::invalid_argument("invalid case");
  }
}

void checks(
  update_theta_sketch& a,
  update_theta_sketch& b,
  double expectedIntersectTheta,
  uint32_t expectedIntersectCount,
  bool expectedIntersectEmpty,
  double expectedAnotbTheta,
  uint32_t expectedAnotbCount,
  bool expectedAnotbEmpty,
  double expectedUnionTheta,
  uint32_t expectedUnionCount,
  bool expectedUnionEmpty
) {
  {
    theta_intersection inter;
    inter.update(a);
    inter.update(b);
    auto csk = inter.get_result();
    check_result("Intersection update sketches", csk, expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty);
  }
  {
    theta_intersection inter;
    inter.update(a.compact());
    inter.update(b.compact());
    auto csk = inter.get_result();
    check_result("Intersection compact sketches", csk, expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty);
  }
  {
    theta_a_not_b a_not_b;
    auto csk = a_not_b.compute(a, b);
    check_result("AnotB update sketches", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
  }
  {
    theta_a_not_b a_not_b;
    auto csk = a_not_b.compute(a.compact(), b.compact());
    check_result("AnotB compact sketches", csk, expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty);
  }
  {
    auto u = theta_union::builder().build();
    u.update(a);
    u.update(b);
    auto csk = u.get_result();
    check_result("Union update sketches", csk, expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
  }
}


TEST_CASE("empty empty") {
  auto a = build_sketch(SkType::EMPTY, 0, 0);
  auto b = build_sketch(SkType::EMPTY, 0, 0);
  const double expectedIntersectTheta = 1.0;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = true;
  const double expectedAnotbTheta = 1.0;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = true;
  const double expectedUnionTheta = 1.0;
  const int expectedUnionCount = 0;
  const bool expectedUnionEmpty = true;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("empty exact") {
  auto a = build_sketch(SkType::EMPTY, 0, 0);
  auto b = build_sketch(SkType::EXACT, 0, GT_MIDP_V);
  const double expectedIntersectTheta = 1.0;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = true;
  const double expectedAnotbTheta = 1.0;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = true;
  const double expectedUnionTheta = 1.0;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("empty degenerate") {
  auto a = build_sketch(SkType::EMPTY, 0, 0);
  auto b = build_sketch(SkType::DEGENERATE, LOWP, GT_LOWP_V);
  const double expectedIntersectTheta = 1.0;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = true;
  const double expectedAnotbTheta = 1.0;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = true;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 0;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("empty estimation") {
  auto a = build_sketch(SkType::EMPTY, 0, 0);
  auto b = build_sketch(SkType::ESTIMATION, LOWP, LT_LOWP_V);
  const double expectedIntersectTheta = 1.0;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = true;
  const double expectedAnotbTheta = 1.0;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = true;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

// ---

TEST_CASE("exact empty") {
  auto a = build_sketch(SkType::EXACT, 0, GT_MIDP_V);
  auto b = build_sketch(SkType::EMPTY, 0, 0);
  const double expectedIntersectTheta = 1.0;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = true;
  const double expectedAnotbTheta = 1.0;
  const int expectedAnotbCount = 1;
  const bool expectedAnotbEmpty = false;
  const double expectedUnionTheta = 1.0;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("exact exact") {
  auto a = build_sketch(SkType::EXACT, 0, GT_MIDP_V);
  auto b = build_sketch(SkType::EXACT, 0, GT_MIDP_V);
  const double expectedIntersectTheta = 1.0;
  const int expectedIntersectCount = 1;
  const bool expectedIntersectEmpty = false;
  const double expectedAnotbTheta = 1.0;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = true;
  const double expectedUnionTheta = 1.0;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("exact degenerate") {
  auto a = build_sketch(SkType::EXACT, 0, LT_LOWP_V);
  auto b = build_sketch(SkType::DEGENERATE, LOWP, GT_LOWP_V); //entries = 0
  const double expectedIntersectTheta = LOWP_THETA;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = false;
  const double expectedAnotbTheta = LOWP_THETA;
  const int expectedAnotbCount = 1;
  const bool expectedAnotbEmpty = false;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("exact estimation") {
  auto a = build_sketch(SkType::EXACT, 0, LT_LOWP_V);
  auto b = build_sketch(SkType::ESTIMATION, LOWP, LT_LOWP_V);
  const double expectedIntersectTheta = LOWP_THETA;
  const int expectedIntersectCount = 1;
  const bool expectedIntersectEmpty = false;
  const double expectedAnotbTheta = LOWP_THETA;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = false;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

// ---

TEST_CASE("estimation empty") {
  auto a = build_sketch(SkType::ESTIMATION, LOWP, LT_LOWP_V);
  auto b = build_sketch(SkType::EMPTY, 0, 0);
  const double expectedIntersectTheta = 1.0;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = true;
  const double expectedAnotbTheta = LOWP_THETA;
  const int expectedAnotbCount = 1;
  const bool expectedAnotbEmpty = false;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("estimation exact") {
  auto a = build_sketch(SkType::ESTIMATION, LOWP, LT_LOWP_V);
  auto b = build_sketch(SkType::EXACT, 0, LT_LOWP_V);
  const double expectedIntersectTheta = LOWP_THETA;
  const int expectedIntersectCount = 1;
  const bool expectedIntersectEmpty = false;
  const double expectedAnotbTheta = LOWP_THETA;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = false;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("estimation degenerate") {
  auto a = build_sketch(SkType::ESTIMATION, MIDP, LT_LOWP_V);
  auto b = build_sketch(SkType::DEGENERATE, LOWP, GT_LOWP_V);
  const double expectedIntersectTheta = LOWP_THETA;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = false;
  const double expectedAnotbTheta = LOWP_THETA;
  const int expectedAnotbCount = 1;
  const bool expectedAnotbEmpty = false;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("estimation estimation") {
  auto a = build_sketch(SkType::ESTIMATION, MIDP, LT_LOWP_V);
  auto b = build_sketch(SkType::ESTIMATION, LOWP, LT_LOWP_V);
  const double expectedIntersectTheta = LOWP_THETA;
  const int expectedIntersectCount = 1;
  const bool expectedIntersectEmpty = false;
  const double expectedAnotbTheta = LOWP_THETA;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = false;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

// ---

TEST_CASE("degenerate empty") {
  auto a = build_sketch(SkType::DEGENERATE, LOWP, GT_LOWP_V); //entries = 0
  auto b = build_sketch(SkType::EMPTY, 0, 0);
  const double expectedIntersectTheta = 1.0;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = true;
  const double expectedAnotbTheta = LOWP_THETA;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = false;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 0;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("degenerate exact") {
  auto a = build_sketch(SkType::DEGENERATE, LOWP, GT_LOWP_V); //entries = 0
  auto b = build_sketch(SkType::EXACT, 0, LT_LOWP_V);
  const double expectedIntersectTheta = LOWP_THETA;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = false;
  const double expectedAnotbTheta = LOWP_THETA;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = false;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("degenerate degenerate") {
  auto a = build_sketch(SkType::DEGENERATE, MIDP, GT_MIDP_V); //entries = 0
  auto b = build_sketch(SkType::DEGENERATE, LOWP, GT_LOWP_V);
  const double expectedIntersectTheta = LOWP_THETA;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = false;
  const double expectedAnotbTheta = LOWP_THETA;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = false;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 0;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

TEST_CASE("degenerate estimation") {
  auto a = build_sketch(SkType::DEGENERATE, MIDP, GT_MIDP_V); //entries = 0
  auto b = build_sketch(SkType::ESTIMATION, LOWP, LT_LOWP_V);
  const double expectedIntersectTheta = LOWP_THETA;
  const int expectedIntersectCount = 0;
  const bool expectedIntersectEmpty = false;
  const double expectedAnotbTheta = LOWP_THETA;
  const int expectedAnotbCount = 0;
  const bool expectedAnotbEmpty = false;
  const double expectedUnionTheta = LOWP_THETA;
  const int expectedUnionCount = 1;
  const bool expectedUnionEmpty = false;

  checks(a, b,
      expectedIntersectTheta, expectedIntersectCount, expectedIntersectEmpty,
      expectedAnotbTheta, expectedAnotbCount, expectedAnotbEmpty,
      expectedUnionTheta, expectedUnionCount, expectedUnionEmpty);
}

} /* namespace datasketches */
