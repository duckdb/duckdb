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

#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/util/string.h"
#include "arrow/util/utf8.h"

namespace arrow {
namespace util {

class UTF8Test : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    InitializeUTF8();

    all_valid_sequences.clear();
    for (const auto& v :
         {valid_sequences_1, valid_sequences_2, valid_sequences_3, valid_sequences_4}) {
      all_valid_sequences.insert(all_valid_sequences.end(), v.begin(), v.end());
    }

    all_invalid_sequences.clear();
    for (const auto& v : {invalid_sequences_1, invalid_sequences_2, invalid_sequences_3,
                          invalid_sequences_4}) {
      all_invalid_sequences.insert(all_invalid_sequences.end(), v.begin(), v.end());
    }
  }

  static std::vector<std::string> valid_sequences_1;
  static std::vector<std::string> valid_sequences_2;
  static std::vector<std::string> valid_sequences_3;
  static std::vector<std::string> valid_sequences_4;

  static std::vector<std::string> all_valid_sequences;

  static std::vector<std::string> invalid_sequences_1;
  static std::vector<std::string> invalid_sequences_2;
  static std::vector<std::string> invalid_sequences_3;
  static std::vector<std::string> invalid_sequences_4;

  static std::vector<std::string> all_invalid_sequences;
};

std::vector<std::string> UTF8Test::valid_sequences_1 = {"a", "\x7f"};
std::vector<std::string> UTF8Test::valid_sequences_2 = {"\xc2\x80", "\xc3\xbf",
                                                        "\xdf\xbf"};
std::vector<std::string> UTF8Test::valid_sequences_3 = {"\xe0\xa0\x80", "\xe8\x9d\xa5",
                                                        "\xef\xbf\xbf"};
std::vector<std::string> UTF8Test::valid_sequences_4 = {
    "\xf0\x90\x80\x80", "\xf0\x9f\xbf\xbf", "\xf4\x80\x80\x80", "\xf4\x8f\xbf\xbf"};

std::vector<std::string> UTF8Test::all_valid_sequences;

std::vector<std::string> UTF8Test::invalid_sequences_1 = {"\x80", "\xa0", "\xbf", "\xc0",
                                                          "\xc1"};
std::vector<std::string> UTF8Test::invalid_sequences_2 = {
    "\x80\x80", "\x80\xbf", "\xbf\x80", "\xbf\xbf",
    "\xc1\x80", "\xc2\x7f", "\xc3\xff", "\xdf\xc0"};
std::vector<std::string> UTF8Test::invalid_sequences_3 = {
    "\xe0\x80\x80", "\xe0\x9f\x80", "\xef\xbf\xc0", "\xef\xc0\xbf", "\xef\xff\xff",
    // Surrogates
    "\xed\xa0\x80", "\xed\xbf\xbf"};
std::vector<std::string> UTF8Test::invalid_sequences_4 = {
    "\xf0\x80\x80\x80", "\xf0\x8f\x80\x80", "\xf4\x8f\xbf\xc0", "\xf4\x8f\xc0\xbf",
    "\xf4\x90\x80\x80"};

std::vector<std::string> UTF8Test::all_invalid_sequences;

class UTF8ValidationTest : public UTF8Test {};

::testing::AssertionResult IsValidUTF8(const std::string& s) {
  if (ValidateUTF8(reinterpret_cast<const uint8_t*>(s.data()), s.size())) {
    return ::testing::AssertionSuccess();
  } else {
    std::string h = HexEncode(reinterpret_cast<const uint8_t*>(s.data()),
                              static_cast<int32_t>(s.size()));
    return ::testing::AssertionFailure()
           << "string '" << h << "' didn't validate as UTF8";
  }
}

::testing::AssertionResult IsInvalidUTF8(const std::string& s) {
  if (!ValidateUTF8(reinterpret_cast<const uint8_t*>(s.data()), s.size())) {
    return ::testing::AssertionSuccess();
  } else {
    std::string h = HexEncode(reinterpret_cast<const uint8_t*>(s.data()),
                              static_cast<int32_t>(s.size()));
    return ::testing::AssertionFailure() << "string '" << h << "' validated as UTF8";
  }
}

void AssertValidUTF8(const std::string& s) { ASSERT_TRUE(IsValidUTF8(s)); }

void AssertInvalidUTF8(const std::string& s) { ASSERT_TRUE(IsInvalidUTF8(s)); }

TEST_F(UTF8ValidationTest, EmptyString) { AssertValidUTF8(""); }

TEST_F(UTF8ValidationTest, OneCharacterValid) {
  for (const auto& s : all_valid_sequences) {
    AssertValidUTF8(s);
  }
}

TEST_F(UTF8ValidationTest, TwoCharacterValid) {
  for (const auto& s1 : all_valid_sequences) {
    for (const auto& s2 : all_valid_sequences) {
      AssertValidUTF8(s1 + s2);
    }
  }
}

TEST_F(UTF8ValidationTest, RandomValid) {
#ifdef ARROW_VALGRIND
  const int niters = 50;
#else
  const int niters = 1000;
#endif
  const int nchars = 100;
  std::default_random_engine gen(42);
  std::uniform_int_distribution<size_t> valid_dist(0, all_valid_sequences.size() - 1);

  for (int i = 0; i < niters; ++i) {
    std::string s;
    s.reserve(nchars * 4);
    for (int j = 0; j < nchars; ++j) {
      s += all_valid_sequences[valid_dist(gen)];
    }
    AssertValidUTF8(s);
  }
}

TEST_F(UTF8ValidationTest, OneCharacterTruncated) {
  for (const auto& s : all_valid_sequences) {
    if (s.size() > 1) {
      AssertInvalidUTF8(s.substr(0, s.size() - 1));
    }
  }
}

TEST_F(UTF8ValidationTest, TwoCharacterTruncated) {
  for (const auto& s1 : all_valid_sequences) {
    for (const auto& s2 : all_valid_sequences) {
      if (s2.size() > 1) {
        AssertInvalidUTF8(s1 + s2.substr(0, s2.size() - 1));
        AssertInvalidUTF8(s2.substr(0, s2.size() - 1) + s1);
      }
    }
  }
}

TEST_F(UTF8ValidationTest, OneCharacterInvalid) {
  for (const auto& s : all_invalid_sequences) {
    AssertInvalidUTF8(s);
  }
}

TEST_F(UTF8ValidationTest, TwoCharacterInvalid) {
  for (const auto& s1 : all_valid_sequences) {
    for (const auto& s2 : all_invalid_sequences) {
      AssertInvalidUTF8(s1 + s2);
      AssertInvalidUTF8(s2 + s1);
    }
  }
  for (const auto& s1 : all_invalid_sequences) {
    for (const auto& s2 : all_invalid_sequences) {
      AssertInvalidUTF8(s1 + s2);
    }
  }
}

TEST_F(UTF8ValidationTest, RandomInvalid) {
#ifdef ARROW_VALGRIND
  const int niters = 50;
#else
  const int niters = 1000;
#endif
  const int nchars = 100;
  std::default_random_engine gen(42);
  std::uniform_int_distribution<size_t> valid_dist(0, all_valid_sequences.size() - 1);
  std::uniform_int_distribution<int> invalid_pos_dist(0, nchars - 1);
  std::uniform_int_distribution<size_t> invalid_dist(0, all_invalid_sequences.size() - 1);

  for (int i = 0; i < niters; ++i) {
    std::string s;
    s.reserve(nchars * 4);
    // Stuff a single invalid sequence somewhere in a valid UTF8 stream
    int invalid_pos = invalid_pos_dist(gen);
    for (int j = 0; j < nchars; ++j) {
      if (j == invalid_pos) {
        s += all_invalid_sequences[invalid_dist(gen)];
      } else {
        s += all_valid_sequences[valid_dist(gen)];
      }
    }
    AssertInvalidUTF8(s);
  }
}

TEST_F(UTF8ValidationTest, RandomTruncated) {
#ifdef ARROW_VALGRIND
  const int niters = 50;
#else
  const int niters = 1000;
#endif
  const int nchars = 100;
  std::default_random_engine gen(42);
  std::uniform_int_distribution<size_t> valid_dist(0, all_valid_sequences.size() - 1);
  std::uniform_int_distribution<int> invalid_pos_dist(0, nchars - 1);

  for (int i = 0; i < niters; ++i) {
    std::string s;
    s.reserve(nchars * 4);
    // Truncate a single sequence somewhere in a valid UTF8 stream
    int invalid_pos = invalid_pos_dist(gen);
    for (int j = 0; j < nchars; ++j) {
      if (j == invalid_pos) {
        while (true) {
          // Ensure we truncate a 2-byte or more sequence
          const std::string& t = all_valid_sequences[valid_dist(gen)];
          if (t.size() > 1) {
            s += t.substr(0, t.size() - 1);
            break;
          }
        }
      } else {
        s += all_valid_sequences[valid_dist(gen)];
      }
    }
    AssertInvalidUTF8(s);
  }
}

}  // namespace util
}  // namespace arrow
