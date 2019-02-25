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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/trie.h"

namespace arrow {
namespace internal {

TEST(SmallString, Basics) {
  using SS = SmallString<5>;
  {
    SS s;
    ASSERT_EQ(s.length(), 0);
    ASSERT_EQ(util::string_view(s), util::string_view(""));
    ASSERT_EQ(s, "");
    ASSERT_NE(s, "x");
    ASSERT_EQ(sizeof(s), 6);
  }
  {
    SS s("abc");
    ASSERT_EQ(s.length(), 3);
    ASSERT_EQ(util::string_view(s), util::string_view("abc"));
    ASSERT_EQ(std::memcmp(s.data(), "abc", 3), 0);
    ASSERT_EQ(s, "abc");
    ASSERT_NE(s, "ab");
  }
}

TEST(SmallString, Assign) {
  using SS = SmallString<5>;
  auto s = SS();

  s = util::string_view("abc");
  ASSERT_EQ(s.length(), 3);
  ASSERT_EQ(util::string_view(s), util::string_view("abc"));
  ASSERT_EQ(std::memcmp(s.data(), "abc", 3), 0);
  ASSERT_EQ(s, "abc");
  ASSERT_NE(s, "ab");

  s = std::string("ghijk");
  ASSERT_EQ(s.length(), 5);
  ASSERT_EQ(util::string_view(s), util::string_view("ghijk"));
  ASSERT_EQ(std::memcmp(s.data(), "ghijk", 5), 0);
  ASSERT_EQ(s, "ghijk");
  ASSERT_NE(s, "");

  s = SS("xy");
  ASSERT_EQ(s.length(), 2);
  ASSERT_EQ(util::string_view(s), util::string_view("xy"));
  ASSERT_EQ(std::memcmp(s.data(), "xy", 2), 0);
  ASSERT_EQ(s, "xy");
  ASSERT_NE(s, "xyz");
}

TEST(SmallString, Substr) {
  using SS = SmallString<5>;
  {
    auto s = SS();
    ASSERT_EQ(s.substr(0), "");
    ASSERT_EQ(s.substr(0, 2), "");
  }
  {
    auto s = SS("abcd");
    ASSERT_EQ(s.substr(0), "abcd");
    ASSERT_EQ(s.substr(1), "bcd");
    ASSERT_EQ(s.substr(4), "");
    ASSERT_EQ(s.substr(0, 0), "");
    ASSERT_EQ(s.substr(0, 3), "abc");
    ASSERT_EQ(s.substr(0, 4), "abcd");
    ASSERT_EQ(s.substr(1, 0), "");
    ASSERT_EQ(s.substr(1, 2), "bc");
    ASSERT_EQ(s.substr(4, 0), "");
    ASSERT_EQ(s.substr(4, 1), "");
  }
}

static std::vector<std::string> AllNulls() {
  return {"#N/A",    "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND",
          "1.#QNAN", "N/A",      "NA",  "NULL",    "NaN",      "n/a",  "nan",  "null"};
}

static void TestTrieContents(const Trie& trie, const std::vector<std::string>& entries) {
  std::unordered_map<std::string, int32_t> control;
  auto n_entries = static_cast<int32_t>(entries.size());

  // Build control container
  for (int32_t i = 0; i < n_entries; ++i) {
    auto p = control.insert({entries[i], i});
    ASSERT_TRUE(p.second);
  }

  // Check all existing entries in trie
  for (int32_t i = 0; i < n_entries; ++i) {
    ASSERT_EQ(i, trie.Find(entries[i])) << "for string '" << entries[i] << "'";
  }

  auto CheckNotExists = [&control, &trie](const std::string& s) {
    auto p = control.find(s);
    if (p == control.end()) {
      ASSERT_EQ(-1, trie.Find(s)) << "for string '" << s << "'";
    }
  };

  // Check potentially non-existing strings
  CheckNotExists("");
  CheckNotExists("X");
  CheckNotExists("abcdefxxxxxxxxxxxxxxx");

  // Check potentially non-existing variations of existing entries
  for (const auto& e : entries) {
    CheckNotExists(e + "X");
    if (e.size() > 0) {
      CheckNotExists(e.substr(0, 1));
      auto prefix = e.substr(0, e.size() - 1);
      CheckNotExists(prefix);
      CheckNotExists(prefix + "X");
      auto split_at = e.size() / 2;
      CheckNotExists(e.substr(0, split_at) + 'x' + e.substr(split_at + 1));
    }
  }
}

static void TestTrieContents(const std::vector<std::string>& entries) {
  TrieBuilder builder;
  for (const auto& s : entries) {
    ASSERT_OK(builder.Append(s));
  }
  const Trie trie = builder.Finish();
  ASSERT_OK(trie.Validate());

  TestTrieContents(trie, entries);
}

TEST(Trie, Empty) {
  TrieBuilder builder;
  const Trie trie = builder.Finish();
  ASSERT_OK(trie.Validate());

  ASSERT_EQ(-1, trie.Find(""));
  ASSERT_EQ(-1, trie.Find("x"));
}

TEST(Trie, EmptyString) {
  TrieBuilder builder;
  ASSERT_OK(builder.Append(""));
  const Trie trie = builder.Finish();
  ASSERT_OK(trie.Validate());

  ASSERT_EQ(0, trie.Find(""));
  ASSERT_EQ(-1, trie.Find("x"));
}

TEST(Trie, Basics1) {
  TestTrieContents({"abc", "de", "f"});
  TestTrieContents({"abc", "de", "f", ""});
}

TEST(Trie, Basics2) {
  TestTrieContents({"a", "abc", "abcd", "abcdef"});
  TestTrieContents({"", "a", "abc", "abcd", "abcdef"});
}

TEST(Trie, Basics3) {
  TestTrieContents({"abcd", "ab", "a"});
  TestTrieContents({"abcd", "ab", "a", ""});
}

TEST(Trie, LongStrings) {
  TestTrieContents({"abcdefghijklmnopqr", "abcdefghijklmnoprq", "defghijklmnopqrst"});
  TestTrieContents({"abcdefghijklmnopqr", "abcdefghijklmnoprq", "abcde"});
}

TEST(Trie, NullChars) {
  const std::string empty;
  const std::string nul(1, '\x00');
  std::string a, b, c, d;
  a = "x" + nul + "y";
  b = "x" + nul + "z";
  c = nul + "y";
  d = nul;
  ASSERT_EQ(a.length(), 3);
  ASSERT_EQ(d.length(), 1);

  TestTrieContents({a, b, c, d});
  TestTrieContents({a, b, c});
  TestTrieContents({a, b, c, d, ""});
  TestTrieContents({a, b, c, ""});
  TestTrieContents({d, c, b, a});
  TestTrieContents({c, b, a});
  TestTrieContents({d, c, b, a, ""});
  TestTrieContents({c, b, a, ""});
}

TEST(Trie, NegativeChars) {
  // Test with characters >= 0x80 (to check the absence of sign issues)
  TestTrieContents({"\x7f\x80\x81\xff", "\x7f\x80\x81", "\x7f\xff\x81", "\xff\x80\x81"});
}

TEST(Trie, CSVNulls) { TestTrieContents(AllNulls()); }

TEST(Trie, Duplicates) {
  {
    TrieBuilder builder;
    ASSERT_OK(builder.Append("ab"));
    ASSERT_OK(builder.Append("abc"));
    ASSERT_RAISES(Invalid, builder.Append("abc"));
    ASSERT_OK(builder.Append("abcd"));
    ASSERT_RAISES(Invalid, builder.Append("ab"));
    ASSERT_OK(builder.Append("abcde"));
    const Trie trie = builder.Finish();

    TestTrieContents(trie, {"ab", "abc", "abcd", "abcde"});
  }
  {
    // With allow_duplicates = true
    TrieBuilder builder;
    ASSERT_OK(builder.Append("ab", true));
    ASSERT_OK(builder.Append("abc", true));
    ASSERT_OK(builder.Append("abc", true));
    ASSERT_OK(builder.Append("abcd", true));
    ASSERT_OK(builder.Append("ab", true));
    ASSERT_OK(builder.Append("abcde", true));
    const Trie trie = builder.Finish();

    TestTrieContents(trie, {"ab", "abc", "abcd", "abcde"});
  }
}

TEST(Trie, CapacityError) {
  // A trie uses 16-bit indices into various internal structures and
  // therefore has limited size available.
  TrieBuilder builder;
  uint8_t first, second, third;
  bool had_capacity_error = false;
  uint8_t s[] = "\x00\x00\x00\x00";

  for (first = 1; first < 125; ++first) {
    s[0] = first;
    for (second = 1; second < 125; ++second) {
      s[1] = second;
      for (third = 1; third < 125; ++third) {
        s[2] = third;
        auto st = builder.Append(reinterpret_cast<const char*>(s));
        if (st.IsCapacityError()) {
          DCHECK_GE(first, 2);
          had_capacity_error = true;
          break;
        } else {
          ASSERT_OK(st);
        }
      }
    }
  }
  ASSERT_TRUE(had_capacity_error) << "Should have produced CapacityError";
}

}  // namespace internal
}  // namespace arrow
