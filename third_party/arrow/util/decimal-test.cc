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
#include <array>
#include <cmath>
#include <cstdint>
#include <string>
#include <tuple>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/macros.h"

namespace arrow {

class DecimalTestFixture : public ::testing::Test {
 public:
  DecimalTestFixture() : integer_value_(23423445), string_value_("234.23445") {}
  Decimal128 integer_value_;
  std::string string_value_;
};

TEST_F(DecimalTestFixture, TestToString) {
  Decimal128 decimal(this->integer_value_);
  int32_t scale = 5;
  std::string result = decimal.ToString(scale);
  ASSERT_EQ(result, this->string_value_);
}

TEST_F(DecimalTestFixture, TestFromString) {
  Decimal128 expected(this->integer_value_);
  Decimal128 result;
  int32_t precision, scale;
  ASSERT_OK(Decimal128::FromString(this->string_value_, &result, &precision, &scale));
  ASSERT_EQ(result, expected);
  ASSERT_EQ(precision, 8);
  ASSERT_EQ(scale, 5);
}

TEST_F(DecimalTestFixture, TestStringStartingWithPlus) {
  std::string plus_value("+234.234");
  Decimal128 out;
  int32_t scale;
  int32_t precision;
  ASSERT_OK(Decimal128::FromString(plus_value, &out, &precision, &scale));
  ASSERT_EQ(234234, out);
  ASSERT_EQ(6, precision);
  ASSERT_EQ(3, scale);
}

TEST_F(DecimalTestFixture, TestStringStartingWithPlus128) {
  std::string plus_value("+2342394230592.232349023094");
  Decimal128 expected_value("2342394230592232349023094");
  Decimal128 out;
  int32_t scale;
  int32_t precision;
  ASSERT_OK(Decimal128::FromString(plus_value, &out, &precision, &scale));
  ASSERT_EQ(expected_value, out);
  ASSERT_EQ(25, precision);
  ASSERT_EQ(12, scale);
}

TEST(DecimalTest, TestFromStringDecimal128) {
  std::string string_value("-23049223942343532412");
  Decimal128 result(string_value);
  Decimal128 expected(static_cast<int64_t>(-230492239423435324));
  ASSERT_EQ(result, expected * 100 - 12);

  // Sanity check that our number is actually using more than 64 bits
  ASSERT_NE(result.high_bits(), 0);
}

TEST(DecimalTest, TestFromDecimalString128) {
  std::string string_value("-23049223942343.532412");
  Decimal128 result;
  ASSERT_OK(Decimal128::FromString(string_value, &result));
  Decimal128 expected(static_cast<int64_t>(-230492239423435324));
  ASSERT_EQ(result, expected * 100 - 12);

  // Sanity check that our number is actually using more than 64 bits
  ASSERT_NE(result.high_bits(), 0);
}

TEST(DecimalTest, TestDecimal32SignedRoundTrip) {
  Decimal128 expected("-3402692");

  auto bytes = expected.ToBytes();
  Decimal128 result(bytes.data());
  ASSERT_EQ(expected, result);
}

TEST(DecimalTest, TestDecimal64SignedRoundTrip) {
  Decimal128 expected;
  std::string string_value("-34034293045.921");
  ASSERT_OK(Decimal128::FromString(string_value, &expected));

  auto bytes = expected.ToBytes();
  Decimal128 result(bytes.data());

  ASSERT_EQ(expected, result);
}

TEST(DecimalTest, TestDecimalStringAndBytesRoundTrip) {
  Decimal128 expected;
  std::string string_value("-340282366920938463463374607431.711455");
  ASSERT_OK(Decimal128::FromString(string_value, &expected));

  std::string expected_string_value("-340282366920938463463374607431711455");
  Decimal128 expected_underlying_value(expected_string_value);

  ASSERT_EQ(expected, expected_underlying_value);

  auto bytes = expected.ToBytes();

  Decimal128 result(bytes.data());

  ASSERT_EQ(expected, result);
}

TEST(DecimalTest, TestInvalidInputMinus) {
  std::string invalid_value("-");
  Decimal128 out;
  Status status = Decimal128::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputDot) {
  std::string invalid_value("0.0.0");
  Decimal128 out;
  Status status = Decimal128::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputEmbeddedMinus) {
  std::string invalid_value("0-13-32");
  Decimal128 out;
  Status status = Decimal128::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputSingleChar) {
  std::string invalid_value("a");
  Decimal128 out;
  Status status = Decimal128::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputWithValidSubstring) {
  std::string invalid_value("-23092.235-");
  Decimal128 out;
  Status status = Decimal128::FromString(invalid_value, &out);
  auto msg = status.message();
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputWithMinusPlus) {
  std::string invalid_value("-+23092.235");
  Decimal128 out;
  Status status = Decimal128::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputWithPlusMinus) {
  std::string invalid_value("+-23092.235");
  Decimal128 out;
  Status status = Decimal128::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputWithLeadingZeros) {
  std::string invalid_value("00a");
  Decimal128 out;
  Status status = Decimal128::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalZerosTest, LeadingZerosNoDecimalPoint) {
  std::string string_value("0000000");
  Decimal128 d;
  int32_t precision;
  int32_t scale;
  ASSERT_OK(Decimal128::FromString(string_value, &d, &precision, &scale));
  ASSERT_EQ(0, precision);
  ASSERT_EQ(0, scale);
  ASSERT_EQ(0, d);
}

TEST(DecimalZerosTest, LeadingZerosDecimalPoint) {
  std::string string_value("000.0000");
  Decimal128 d;
  int32_t precision;
  int32_t scale;
  ASSERT_OK(Decimal128::FromString(string_value, &d, &precision, &scale));
  ASSERT_EQ(4, precision);
  ASSERT_EQ(4, scale);
  ASSERT_EQ(0, d);
}

TEST(DecimalZerosTest, NoLeadingZerosDecimalPoint) {
  std::string string_value(".00000");
  Decimal128 d;
  int32_t precision;
  int32_t scale;
  ASSERT_OK(Decimal128::FromString(string_value, &d, &precision, &scale));
  ASSERT_EQ(5, precision);
  ASSERT_EQ(5, scale);
  ASSERT_EQ(0, d);
}

template <typename T>
class Decimal128Test : public ::testing::Test {
 public:
  Decimal128Test() : value_(42) {}
  const T value_;
};

using Decimal128Types =
    ::testing::Types<char, unsigned char, short, unsigned short,  // NOLINT
                     int, unsigned int, long, unsigned long,      // NOLINT
                     long long, unsigned long long                // NOLINT
                     >;

TYPED_TEST_CASE(Decimal128Test, Decimal128Types);

TYPED_TEST(Decimal128Test, ConstructibleFromAnyIntegerType) {
  Decimal128 value(this->value_);
  ASSERT_EQ(42, value.low_bits());
}

TEST(Decimal128TestTrue, ConstructibleFromBool) {
  Decimal128 value(true);
  ASSERT_EQ(1, value.low_bits());
}

TEST(Decimal128TestFalse, ConstructibleFromBool) {
  Decimal128 value(false);
  ASSERT_EQ(0, value.low_bits());
}

TEST(Decimal128Test, Division) {
  const std::string expected_string_value("-23923094039234029");
  const Decimal128 value(expected_string_value);
  const Decimal128 result(value / 3);
  const Decimal128 expected_value("-7974364679744676");
  ASSERT_EQ(expected_value, result);
}

TEST(Decimal128Test, PrintLargePositiveValue) {
  const std::string string_value("99999999999999999999999999999999999999");
  const Decimal128 value(string_value);
  const std::string printed_value = value.ToIntegerString();
  ASSERT_EQ(string_value, printed_value);
}

TEST(Decimal128Test, PrintLargeNegativeValue) {
  const std::string string_value("-99999999999999999999999999999999999999");
  const Decimal128 value(string_value);
  const std::string printed_value = value.ToIntegerString();
  ASSERT_EQ(string_value, printed_value);
}

TEST(Decimal128Test, PrintMaxValue) {
  const std::string string_value("170141183460469231731687303715884105727");
  const Decimal128 value(string_value);
  const std::string printed_value = value.ToIntegerString();
  ASSERT_EQ(string_value, printed_value);
}

TEST(Decimal128Test, PrintMinValue) {
  const std::string string_value("-170141183460469231731687303715884105728");
  const Decimal128 value(string_value);
  const std::string printed_value = value.ToIntegerString();
  ASSERT_EQ(string_value, printed_value);
}

class Decimal128PrintingTest
    : public ::testing::TestWithParam<std::tuple<int32_t, int32_t, std::string>> {};

TEST_P(Decimal128PrintingTest, Print) {
  int32_t test_value;
  int32_t scale;
  std::string expected_string;
  std::tie(test_value, scale, expected_string) = GetParam();
  const Decimal128 value(test_value);
  const std::string printed_value = value.ToString(scale);
  ASSERT_EQ(expected_string, printed_value);
}

INSTANTIATE_TEST_CASE_P(Decimal128PrintingTest, Decimal128PrintingTest,
                        ::testing::Values(std::make_tuple(123, 1, "12.3"),
                                          std::make_tuple(123, 5, "0.00123"),
                                          std::make_tuple(123, 10, "1.23E-8"),
                                          std::make_tuple(123, -1, "1.23E+3"),
                                          std::make_tuple(-123, -1, "-1.23E+3"),
                                          std::make_tuple(123, -3, "1.23E+5"),
                                          std::make_tuple(-123, -3, "-1.23E+5"),
                                          std::make_tuple(12345, -3, "1.2345E+7")));

class Decimal128ParsingTest
    : public ::testing::TestWithParam<std::tuple<std::string, uint64_t, int32_t>> {};

TEST_P(Decimal128ParsingTest, Parse) {
  std::string test_string;
  uint64_t expected_low_bits;
  int32_t expected_scale;
  std::tie(test_string, expected_low_bits, expected_scale) = GetParam();
  Decimal128 value;
  int32_t scale;
  ASSERT_OK(Decimal128::FromString(test_string, &value, NULLPTR, &scale));
  ASSERT_EQ(value.low_bits(), expected_low_bits);
  ASSERT_EQ(expected_scale, scale);
}

INSTANTIATE_TEST_CASE_P(Decimal128ParsingTest, Decimal128ParsingTest,
                        ::testing::Values(std::make_tuple("12.3", 123ULL, 1),
                                          std::make_tuple("0.00123", 123ULL, 5),
                                          std::make_tuple("1.23E-8", 123ULL, 10),
                                          std::make_tuple("-1.23E-8", -123LL, 10),
                                          std::make_tuple("1.23E+3", 1230ULL, 0),
                                          std::make_tuple("-1.23E+3", -1230LL, 0),
                                          std::make_tuple("1.23E+5", 123000ULL, 0),
                                          std::make_tuple("1.2345E+7", 12345000ULL, 0),
                                          std::make_tuple("1.23e-8", 123ULL, 10),
                                          std::make_tuple("-1.23e-8", -123LL, 10),
                                          std::make_tuple("1.23e+3", 1230ULL, 0),
                                          std::make_tuple("-1.23e+3", -1230LL, 0),
                                          std::make_tuple("1.23e+5", 123000ULL, 0),
                                          std::make_tuple("1.2345e+7", 12345000ULL, 0)));

class Decimal128ParsingTestInvalid : public ::testing::TestWithParam<std::string> {};

TEST_P(Decimal128ParsingTestInvalid, Parse) {
  std::string test_string = GetParam();
  Decimal128 value;
  ASSERT_RAISES(Invalid, Decimal128::FromString(test_string, &value));
}

INSTANTIATE_TEST_CASE_P(Decimal128ParsingTestInvalid, Decimal128ParsingTestInvalid,
                        ::testing::Values("0.00123D/3", "1.23eA8", "1.23E+3A",
                                          "-1.23E--5", "1.2345E+++07"));

TEST(Decimal128ParseTest, WithExponentAndNullptrScale) {
  Decimal128 value;
  ASSERT_OK(Decimal128::FromString("1.23E-8", &value));

  const Decimal128 expected_value(123);
  ASSERT_EQ(expected_value, value);
}

TEST(Decimal128Test, TestSmallNumberFormat) {
  Decimal128 value("0.2");
  std::string expected("0.2");

  const int32_t scale = 1;
  std::string result = value.ToString(scale);
  ASSERT_EQ(expected, result);
}

TEST(Decimal128Test, TestNoDecimalPointExponential) {
  Decimal128 value;
  int32_t precision;
  int32_t scale;
  ASSERT_OK(Decimal128::FromString("1E1", &value, &precision, &scale));
  ASSERT_EQ(10, value.low_bits());
  ASSERT_EQ(2, precision);
  ASSERT_EQ(0, scale);
}

TEST(Decimal128Test, TestFromBigEndian) {
  // We test out a variety of scenarios:
  //
  // * Positive values that are left shifted
  //   and filled in with the same bit pattern
  // * Negated of the positive values
  // * Complement of the positive values
  //
  // For the positive values, we can call FromBigEndian
  // with a length that is less than 16, whereas we must
  // pass all 16 bytes for the negative and complement.
  //
  // We use a number of bit patterns to increase the coverage
  // of scenarios
  for (int32_t start : {1, 15, /* 00001111 */
                        85,    /* 01010101 */
                        127 /* 01111111 */}) {
    Decimal128 value(start);
    for (int ii = 0; ii < 16; ++ii) {
      auto little_endian = value.ToBytes();
      std::reverse(little_endian.begin(), little_endian.end());
      Decimal128 out;
      // Limit the number of bytes we are passing to make
      // sure that it works correctly. That's why all of the
      // 'start' values don't have a 1 in the most significant
      // bit place
      ASSERT_OK(Decimal128::FromBigEndian(little_endian.data() + 15 - ii, ii + 1, &out));
      ASSERT_EQ(value, out);

      // Negate it and convert to big endian
      auto negated = -value;
      little_endian = negated.ToBytes();
      std::reverse(little_endian.begin(), little_endian.end());
      // The sign bit is looked up in the MSB
      ASSERT_OK(Decimal128::FromBigEndian(little_endian.data() + 15 - ii, ii + 1, &out));
      ASSERT_EQ(negated, out);

      // Take the complement and convert to big endian
      auto complement = ~value;
      little_endian = complement.ToBytes();
      std::reverse(little_endian.begin(), little_endian.end());
      ASSERT_OK(Decimal128::FromBigEndian(little_endian.data(), 16, &out));
      ASSERT_EQ(complement, out);

      value <<= 8;
      value += Decimal128(start);
    }
  }
}

TEST(Decimal128Test, TestFromBigEndianBadLength) {
  Decimal128 out;
  ASSERT_RAISES(Invalid, Decimal128::FromBigEndian(0, -1, &out));
  ASSERT_RAISES(Invalid, Decimal128::FromBigEndian(0, 17, &out));
}

TEST(Decimal128Test, TestToInteger) {
  Decimal128 value1("1234");
  int32_t out1;

  Decimal128 value2("-1234");
  int64_t out2;

  ASSERT_OK(value1.ToInteger(&out1));
  ASSERT_EQ(1234, out1);

  ASSERT_OK(value1.ToInteger(&out2));
  ASSERT_EQ(1234, out2);

  ASSERT_OK(value2.ToInteger(&out1));
  ASSERT_EQ(-1234, out1);

  ASSERT_OK(value2.ToInteger(&out2));
  ASSERT_EQ(-1234, out2);

  Decimal128 invalid_int32(static_cast<int64_t>(std::pow(2, 31)));
  ASSERT_RAISES(Invalid, invalid_int32.ToInteger(&out1));

  Decimal128 invalid_int64("12345678912345678901");
  ASSERT_RAISES(Invalid, invalid_int64.ToInteger(&out2));
}

TEST(Decimal128Test, Multiply) {
  Decimal128 result;

  result = Decimal128("301") * Decimal128("201");
  ASSERT_EQ(result.ToIntegerString(), "60501");

  result = Decimal128("-301") * Decimal128("201");
  ASSERT_EQ(result.ToIntegerString(), "-60501");

  result = Decimal128("301") * Decimal128("-201");
  ASSERT_EQ(result.ToIntegerString(), "-60501");

  result = Decimal128("-301") * Decimal128("-201");
  ASSERT_EQ(result.ToIntegerString(), "60501");
}

TEST(Decimal128Test, GetWholeAndFraction) {
  Decimal128 value("123456");
  Decimal128 whole;
  Decimal128 fraction;
  int32_t out;

  value.GetWholeAndFraction(0, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(123456, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(0, out);

  value.GetWholeAndFraction(1, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(12345, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(6, out);

  value.GetWholeAndFraction(5, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(1, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(23456, out);

  value.GetWholeAndFraction(7, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(0, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(123456, out);
}

TEST(Decimal128Test, GetWholeAndFractionNegative) {
  Decimal128 value("-123456");
  Decimal128 whole;
  Decimal128 fraction;
  int32_t out;

  value.GetWholeAndFraction(0, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(-123456, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(0, out);

  value.GetWholeAndFraction(1, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(-12345, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(-6, out);

  value.GetWholeAndFraction(5, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(-1, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(-23456, out);

  value.GetWholeAndFraction(7, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(0, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(-123456, out);
}

TEST(Decimal128Test, IncreaseScale) {
  Decimal128 result;
  int32_t out;

  result = Decimal128("1234").IncreaseScaleBy(3);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(1234000, out);

  result = Decimal128("-1234").IncreaseScaleBy(3);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1234000, out);
}

TEST(Decimal128Test, ReduceScaleAndRound) {
  Decimal128 result;
  int32_t out;

  result = Decimal128("123456").ReduceScaleBy(1, false);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(12345, out);

  result = Decimal128("123456").ReduceScaleBy(1, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(12346, out);

  result = Decimal128("123451").ReduceScaleBy(1, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(12345, out);

  result = Decimal128("-123789").ReduceScaleBy(2, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1238, out);

  result = Decimal128("-123749").ReduceScaleBy(2, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1237, out);

  result = Decimal128("-123750").ReduceScaleBy(2, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1238, out);
}

}  // namespace arrow
