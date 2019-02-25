// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "benchmark/benchmark.h"

#include <string>
#include <type_traits>
#include <vector>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/utf8.h"

namespace arrow {
namespace util {

static const char* tiny_valid_ascii = "characters";
static const char* tiny_valid_non_ascii = "caractères";

static const char* valid_ascii =
    "UTF-8 is a variable width character encoding capable of encoding all 1,112,064 "
    "valid code points in Unicode using one to four 8-bit bytes";
static const char* valid_almost_ascii =
    "UTF-8 est un codage de caractères informatiques conçu pour coder l’ensemble des "
    "caractères du « répertoire universel de caractères codés »";
static const char* valid_non_ascii =
    "UTF-8 はISO/IEC 10646 (UCS) "
    "とUnicodeで使える8ビット符号単位の文字符号化形式及び文字符号化スキーム。 ";

static std::string MakeLargeString(const std::string& base, int64_t nbytes) {
  int64_t nrepeats = (nbytes + base.size() - 1) / base.size();
  std::string s;
  s.reserve(nrepeats * nbytes);
  for (int64_t i = 0; i < nrepeats; ++i) {
    s += base;
  }
  return s;
}

static void BenchmarkUTF8Validation(
    benchmark::State& state,  // NOLINT non-const reference
    const std::string& s, bool expected) {
  auto data = reinterpret_cast<const uint8_t*>(s.data());
  auto data_size = static_cast<int64_t>(s.size());

  InitializeUTF8();
  bool b = ValidateUTF8(data, data_size);
  if (b != expected) {
    std::cerr << "Unexpected validation result" << std::endl;
    std::abort();
  }

  while (state.KeepRunning()) {
    bool b = ValidateUTF8(data, data_size);
    benchmark::DoNotOptimize(b);
  }
  state.SetBytesProcessed(state.iterations() * s.size());
}

static void BM_ValidateTinyAscii(benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkUTF8Validation(state, tiny_valid_ascii, true);
}

static void BM_ValidateTinyNonAscii(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkUTF8Validation(state, tiny_valid_non_ascii, true);
}

static void BM_ValidateSmallAscii(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkUTF8Validation(state, valid_ascii, true);
}

static void BM_ValidateSmallAlmostAscii(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkUTF8Validation(state, valid_almost_ascii, true);
}

static void BM_ValidateSmallNonAscii(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkUTF8Validation(state, valid_non_ascii, true);
}

static void BM_ValidateLargeAscii(
    benchmark::State& state) {  // NOLINT non-const reference
  auto s = MakeLargeString(valid_ascii, 100000);
  BenchmarkUTF8Validation(state, s, true);
}

static void BM_ValidateLargeAlmostAscii(
    benchmark::State& state) {  // NOLINT non-const reference
  auto s = MakeLargeString(valid_almost_ascii, 100000);
  BenchmarkUTF8Validation(state, s, true);
}

static void BM_ValidateLargeNonAscii(
    benchmark::State& state) {  // NOLINT non-const reference
  auto s = MakeLargeString(valid_non_ascii, 100000);
  BenchmarkUTF8Validation(state, s, true);
}

static const int kRepetitions = 1;

BENCHMARK(BM_ValidateTinyAscii)->Repetitions(kRepetitions);
BENCHMARK(BM_ValidateTinyNonAscii)->Repetitions(kRepetitions);
BENCHMARK(BM_ValidateSmallAscii)->Repetitions(kRepetitions);
BENCHMARK(BM_ValidateSmallAlmostAscii)->Repetitions(kRepetitions);
BENCHMARK(BM_ValidateSmallNonAscii)->Repetitions(kRepetitions);
BENCHMARK(BM_ValidateLargeAscii)->Repetitions(kRepetitions);
BENCHMARK(BM_ValidateLargeAlmostAscii)->Repetitions(kRepetitions);
BENCHMARK(BM_ValidateLargeNonAscii)->Repetitions(kRepetitions);

}  // namespace util
}  // namespace arrow
