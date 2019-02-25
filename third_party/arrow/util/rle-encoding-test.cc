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

// From Apache Impala (incubating) as of 2016-01-29

#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include <boost/utility.hpp>  // IWYU pragma: export

#include "arrow/util/bit-stream-utils.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/rle-encoding.h"

using std::vector;

namespace arrow {
namespace util {

const int MAX_WIDTH = 32;

TEST(BitArray, TestBool) {
  const int len = 8;
  uint8_t buffer[len];

  BitUtil::BitWriter writer(buffer, len);

  // Write alternating 0's and 1's
  for (int i = 0; i < 8; ++i) {
    bool result = writer.PutValue(i % 2, 1);
    EXPECT_TRUE(result);
  }
  writer.Flush();
  EXPECT_EQ((int)buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));

  // Write 00110011
  for (int i = 0; i < 8; ++i) {
    bool result = false;
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        result = writer.PutValue(false, 1);
        break;
      default:
        result = writer.PutValue(true, 1);
        break;
    }
    EXPECT_TRUE(result);
  }
  writer.Flush();

  // Validate the exact bit value
  EXPECT_EQ((int)buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));
  EXPECT_EQ((int)buffer[1], BOOST_BINARY(1 1 0 0 1 1 0 0));

  // Use the reader and validate
  BitUtil::BitReader reader(buffer, len);
  for (int i = 0; i < 8; ++i) {
    bool val = false;
    bool result = reader.GetValue(1, &val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, (i % 2) != 0);
  }

  for (int i = 0; i < 8; ++i) {
    bool val = false;
    bool result = reader.GetValue(1, &val);
    EXPECT_TRUE(result);
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        EXPECT_EQ(val, false);
        break;
      default:
        EXPECT_EQ(val, true);
        break;
    }
  }
}

// Writes 'num_vals' values with width 'bit_width' and reads them back.
void TestBitArrayValues(int bit_width, int num_vals) {
  int len = static_cast<int>(BitUtil::BytesForBits(bit_width * num_vals));
  EXPECT_GT(len, 0);
  const uint64_t mod = bit_width == 64 ? 1 : 1LL << bit_width;

  std::vector<uint8_t> buffer(len);
  BitUtil::BitWriter writer(buffer.data(), len);
  for (int i = 0; i < num_vals; ++i) {
    bool result = writer.PutValue(i % mod, bit_width);
    EXPECT_TRUE(result);
  }
  writer.Flush();
  EXPECT_EQ(writer.bytes_written(), len);

  BitUtil::BitReader reader(buffer.data(), len);
  for (int i = 0; i < num_vals; ++i) {
    int64_t val = 0;
    bool result = reader.GetValue(bit_width, &val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, i % mod);
  }
  EXPECT_EQ(reader.bytes_left(), 0);
}

TEST(BitArray, TestValues) {
  for (int width = 1; width <= MAX_WIDTH; ++width) {
    TestBitArrayValues(width, 1);
    TestBitArrayValues(width, 2);
    // Don't write too many values
    TestBitArrayValues(width, (width < 12) ? (1 << width) : 4096);
    TestBitArrayValues(width, 1024);
  }
}

// Test some mixed values
TEST(BitArray, TestMixed) {
  const int len = 1024;
  uint8_t buffer[len];
  bool parity = true;

  BitUtil::BitWriter writer(buffer, len);
  for (int i = 0; i < len; ++i) {
    bool result;
    if (i % 2 == 0) {
      result = writer.PutValue(parity, 1);
      parity = !parity;
    } else {
      result = writer.PutValue(i, 10);
    }
    EXPECT_TRUE(result);
  }
  writer.Flush();

  parity = true;
  BitUtil::BitReader reader(buffer, len);
  for (int i = 0; i < len; ++i) {
    bool result;
    if (i % 2 == 0) {
      bool val;
      result = reader.GetValue(1, &val);
      EXPECT_EQ(val, parity);
      parity = !parity;
    } else {
      int val;
      result = reader.GetValue(10, &val);
      EXPECT_EQ(val, i);
    }
    EXPECT_TRUE(result);
  }
}

// Validates encoding of values by encoding and decoding them.  If
// expected_encoding != NULL, also validates that the encoded buffer is
// exactly 'expected_encoding'.
// if expected_len is not -1, it will validate the encoded size is correct.
void ValidateRle(const vector<int>& values, int bit_width, uint8_t* expected_encoding,
                 int expected_len) {
  const int len = 64 * 1024;
  uint8_t buffer[len];
  EXPECT_LE(expected_len, len);

  RleEncoder encoder(buffer, len, bit_width);
  for (size_t i = 0; i < values.size(); ++i) {
    bool result = encoder.Put(values[i]);
    EXPECT_TRUE(result);
  }
  int encoded_len = encoder.Flush();

  if (expected_len != -1) {
    EXPECT_EQ(encoded_len, expected_len);
  }
  if (expected_encoding != NULL) {
    EXPECT_EQ(memcmp(buffer, expected_encoding, encoded_len), 0);
  }

  // Verify read
  {
    RleDecoder decoder(buffer, len, bit_width);
    for (size_t i = 0; i < values.size(); ++i) {
      uint64_t val;
      bool result = decoder.Get(&val);
      EXPECT_TRUE(result);
      EXPECT_EQ(values[i], val);
    }
  }

  // Verify batch read
  {
    RleDecoder decoder(buffer, len, bit_width);
    vector<int> values_read(values.size());
    ASSERT_EQ(values.size(),
              decoder.GetBatch(values_read.data(), static_cast<int>(values.size())));
    EXPECT_EQ(values, values_read);
  }
}

// A version of ValidateRle that round-trips the values and returns false if
// the returned values are not all the same
bool CheckRoundTrip(const vector<int>& values, int bit_width) {
  const int len = 64 * 1024;
  uint8_t buffer[len];
  RleEncoder encoder(buffer, len, bit_width);
  for (size_t i = 0; i < values.size(); ++i) {
    bool result = encoder.Put(values[i]);
    if (!result) {
      return false;
    }
  }
  int encoded_len = encoder.Flush();
  int out = 0;

  {
    RleDecoder decoder(buffer, encoded_len, bit_width);
    for (size_t i = 0; i < values.size(); ++i) {
      EXPECT_TRUE(decoder.Get(&out));
      if (values[i] != out) {
        return false;
      }
    }
  }

  // Verify batch read
  {
    RleDecoder decoder(buffer, len, bit_width);
    vector<int> values_read(values.size());
    if (static_cast<int>(values.size()) !=
        decoder.GetBatch(values_read.data(), static_cast<int>(values.size()))) {
      return false;
    }
    if (values != values_read) {
      return false;
    }
  }

  return true;
}

TEST(Rle, SpecificSequences) {
  const int len = 1024;
  uint8_t expected_buffer[len];
  vector<int> values;

  // Test 50 0' followed by 50 1's
  values.resize(100);
  for (int i = 0; i < 50; ++i) {
    values[i] = 0;
  }
  for (int i = 50; i < 100; ++i) {
    values[i] = 1;
  }

  // expected_buffer valid for bit width <= 1 byte
  expected_buffer[0] = (50 << 1);
  expected_buffer[1] = 0;
  expected_buffer[2] = (50 << 1);
  expected_buffer[3] = 1;
  for (int width = 1; width <= 8; ++width) {
    ValidateRle(values, width, expected_buffer, 4);
  }

  for (int width = 9; width <= MAX_WIDTH; ++width) {
    ValidateRle(values, width, NULL,
                2 * (1 + static_cast<int>(BitUtil::CeilDiv(width, 8))));
  }

  // Test 100 0's and 1's alternating
  for (int i = 0; i < 100; ++i) {
    values[i] = i % 2;
  }
  int num_groups = static_cast<int>(BitUtil::CeilDiv(100, 8));
  expected_buffer[0] = static_cast<uint8_t>((num_groups << 1) | 1);
  for (int i = 1; i <= 100 / 8; ++i) {
    expected_buffer[i] = BOOST_BINARY(1 0 1 0 1 0 1 0);
  }
  // Values for the last 4 0 and 1's. The upper 4 bits should be padded to 0.
  expected_buffer[100 / 8 + 1] = BOOST_BINARY(0 0 0 0 1 0 1 0);

  // num_groups and expected_buffer only valid for bit width = 1
  ValidateRle(values, 1, expected_buffer, 1 + num_groups);
  for (int width = 2; width <= MAX_WIDTH; ++width) {
    int num_values = static_cast<int>(BitUtil::CeilDiv(100, 8)) * 8;
    ValidateRle(values, width, NULL,
                1 + static_cast<int>(BitUtil::CeilDiv(width * num_values, 8)));
  }
}

// ValidateRle on 'num_vals' values with width 'bit_width'. If 'value' != -1, that value
// is used, otherwise alternating values are used.
void TestRleValues(int bit_width, int num_vals, int value = -1) {
  const uint64_t mod = (bit_width == 64) ? 1 : 1LL << bit_width;
  vector<int> values;
  for (int v = 0; v < num_vals; ++v) {
    values.push_back((value != -1) ? value : static_cast<int>(v % mod));
  }
  ValidateRle(values, bit_width, NULL, -1);
}

TEST(Rle, TestValues) {
  for (int width = 1; width <= MAX_WIDTH; ++width) {
    TestRleValues(width, 1);
    TestRleValues(width, 1024);
    TestRleValues(width, 1024, 0);
    TestRleValues(width, 1024, 1);
  }
}

TEST(Rle, BitWidthZeroRepeated) {
  uint8_t buffer[1];
  const int num_values = 15;
  buffer[0] = num_values << 1;  // repeated indicator byte
  RleDecoder decoder(buffer, sizeof(buffer), 0);
  uint8_t val;
  for (int i = 0; i < num_values; ++i) {
    bool result = decoder.Get(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, 0);  // can only encode 0s with bit width 0
  }
  EXPECT_FALSE(decoder.Get(&val));
}

TEST(Rle, BitWidthZeroLiteral) {
  uint8_t buffer[1];
  const int num_groups = 4;
  buffer[0] = num_groups << 1 | 1;  // literal indicator byte
  RleDecoder decoder = RleDecoder(buffer, sizeof(buffer), 0);
  const int num_values = num_groups * 8;
  uint8_t val;
  for (int i = 0; i < num_values; ++i) {
    bool result = decoder.Get(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, 0);  // can only encode 0s with bit width 0
  }
  EXPECT_FALSE(decoder.Get(&val));
}

// Test that writes out a repeated group and then a literal
// group but flush before finishing.
TEST(BitRle, Flush) {
  vector<int> values;
  for (int i = 0; i < 16; ++i) values.push_back(1);
  values.push_back(0);
  ValidateRle(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRle(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRle(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRle(values, 1, NULL, -1);
}

// Test some random sequences.
TEST(BitRle, Random) {
  int niters = 50;
  int ngroups = 1000;
  int max_group_size = 16;
  vector<int> values(ngroups + max_group_size);

  // prng setup
  std::random_device rd;
  std::uniform_int_distribution<int> dist(1, 20);

  for (int iter = 0; iter < niters; ++iter) {
    // generate a seed with device entropy
    uint32_t seed = rd();
    std::mt19937 gen(seed);

    bool parity = 0;
    values.resize(0);

    for (int i = 0; i < ngroups; ++i) {
      int group_size = dist(gen);
      if (group_size > max_group_size) {
        group_size = 1;
      }
      for (int i = 0; i < group_size; ++i) {
        values.push_back(parity);
      }
      parity = !parity;
    }
    if (!CheckRoundTrip(values, BitUtil::NumRequiredBits(values.size()))) {
      FAIL() << "failing seed: " << seed;
    }
  }
}

// Test a sequence of 1 0's, 2 1's, 3 0's. etc
// e.g. 011000111100000
TEST(BitRle, RepeatedPattern) {
  vector<int> values;
  const int min_run = 1;
  const int max_run = 32;

  for (int i = min_run; i <= max_run; ++i) {
    int v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  // And go back down again
  for (int i = max_run; i >= min_run; --i) {
    int v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  ValidateRle(values, 1, NULL, -1);
}

TEST(BitRle, Overflow) {
  for (int bit_width = 1; bit_width < 32; bit_width += 3) {
    int len = RleEncoder::MinBufferSize(bit_width);
    std::vector<uint8_t> buffer(len);
    int num_added = 0;
    bool parity = true;

    RleEncoder encoder(buffer.data(), len, bit_width);
    // Insert alternating true/false until there is no space left
    while (true) {
      bool result = encoder.Put(parity);
      parity = !parity;
      if (!result) break;
      ++num_added;
    }

    int bytes_written = encoder.Flush();
    EXPECT_LE(bytes_written, len);
    EXPECT_GT(num_added, 0);

    RleDecoder decoder(buffer.data(), bytes_written, bit_width);
    parity = true;
    uint32_t v;
    for (int i = 0; i < num_added; ++i) {
      bool result = decoder.Get(&v);
      EXPECT_TRUE(result);
      EXPECT_EQ(v != 0, parity);
      parity = !parity;
    }
    // Make sure we get false when reading past end a couple times.
    EXPECT_FALSE(decoder.Get(&v));
    EXPECT_FALSE(decoder.Get(&v));
  }
}

}  // namespace util
}  // namespace arrow
