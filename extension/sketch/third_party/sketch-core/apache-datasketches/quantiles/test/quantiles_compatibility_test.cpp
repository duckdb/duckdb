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

#include <catch.hpp>
#include <cmath>
#include <sstream>
#include <fstream>

#include <quantiles_sketch.hpp>
#include <serde.hpp>
#include <test_allocator.hpp>

namespace datasketches {

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

// these tests are for compatibility with old versions of Java's
// Quantiles sketch, which is only for doubles.
// 
// typical usage would be just quantiles_sketch<double>, but here we use test_allocator
using quantiles_double_sketch = quantiles_sketch<double, std::less<double>, test_allocator<double>>;

static void quantiles_decode_and_check(uint16_t k, uint64_t n, const std::string& version,
                                       double expected_median) {
  const double median_rank = 0.5;

  std::ostringstream filestr;
  filestr << "Qk" << k << "_n" << n << "_v" << version << ".sk";
  // as stream
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  std::string filename = testBinaryInputPath + filestr.str();

  is.open(filename, std::ios::binary);
  auto sketch_stream = quantiles_double_sketch::deserialize(is, serde<double>(), 0);
  is.close();
  REQUIRE(sketch_stream.get_quantile(median_rank) == expected_median);

  // as bytes
  std::ifstream infile(filename, std::ios::binary);
  std::vector<char> bytes(
    (std::istreambuf_iterator<char>(infile)),
    (std::istreambuf_iterator<char>()));
  infile.close();
  auto sketch_bytes = quantiles_double_sketch::deserialize(bytes.data(), bytes.size(), serde<double>(), 0);
  REQUIRE(sketch_bytes.get_quantile(median_rank) == expected_median);
}

TEST_CASE("quantiles compatibility", "[quantiles_compatibility]") {

  // setup
  test_allocator_total_bytes = 0;

  SECTION("Qk128_n50_v0.3.0.sk") {
    // file: Qk128_n50_v0.3.0.sk
    // median: 26.0
    quantiles_decode_and_check(128, 50, "0.3.0", 26.0);
  }

  SECTION("Qk128_n1000_v0.3.0.sk") {
    // file: Qk128_n1000_v0.3.0.sk
    // median: 501.0
    quantiles_decode_and_check(128, 1000, "0.3.0", 501.0);
  }

  SECTION("Qk128_n50_v0.6.0.sk") {
    // file: Qk128_n50_v0.6.0.sk
    // median: 26.0
    quantiles_decode_and_check(128, 50, "0.6.0", 26.0);
  }

  SECTION("Qk128_n1000_v0.6.0.sk") {
    // file: Qk128_n1000_v0.6.0.sk
    // median: 501.0
    quantiles_decode_and_check(128, 1000, "0.6.0", 501.0);
  }

  SECTION("Qk128_n50_v0.8.0.sk") {
    // file: Qk128_n50_v0.8.0.sk
    // median: 26.0
    quantiles_decode_and_check(128, 50, "0.8.0", 26.0);
  }

  SECTION("Qk128_n1000_v0.8.0.sk") {
    // file: Qk128_n1000_v0.8.0.sk
    // median: 501.0
    quantiles_decode_and_check(128, 1000, "0.8.0", 501.0);
  }

  SECTION("Qk128_n50_v0.8.3.sk") {
    // file: Qk128_n50_v0.8.3.sk
    // median: 26.0
    quantiles_decode_and_check(128, 50, "0.8.3", 26.0);
  }

  SECTION("Qk128_n1000_v0.8.3.sk") {
    // file: Qk128_n1000_v0.8.3.sk
    // median: 501.0
    quantiles_decode_and_check(128, 1000, "0.8.3", 501.0);
  }

  // cleanup
  if (test_allocator_total_bytes != 0) {
    REQUIRE(test_allocator_total_bytes == 0);
  }

}

} /* namespace datasketches */
