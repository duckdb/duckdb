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

#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/pretty_print.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {

class TestPrettyPrint : public ::testing::Test {
 public:
  void SetUp() {}

  void Print(const Array& array) {}

 private:
  std::ostringstream sink_;
};

template <typename T>
void CheckStream(const T& obj, const PrettyPrintOptions& options, const char* expected) {
  std::ostringstream sink;
  ASSERT_OK(PrettyPrint(obj, options, &sink));
  std::string result = sink.str();
  ASSERT_EQ(std::string(expected, strlen(expected)), result);
}

void CheckArray(const Array& arr, const PrettyPrintOptions& options, const char* expected,
                bool check_operator = true) {
  CheckStream(arr, options, expected);

  if (options.indent == 0 && check_operator) {
    std::stringstream ss;
    ss << arr;
    std::string result = std::string(expected, strlen(expected));
    ASSERT_EQ(result, ss.str());
  }
}

template <typename T>
void Check(const T& obj, const PrettyPrintOptions& options, const char* expected) {
  std::string result;
  ASSERT_OK(PrettyPrint(obj, options, &result));
  ASSERT_EQ(std::string(expected, strlen(expected)), result);
}

template <typename TYPE, typename C_TYPE>
void CheckPrimitive(const PrettyPrintOptions& options, const std::vector<bool>& is_valid,
                    const std::vector<C_TYPE>& values, const char* expected,
                    bool check_operator = true) {
  std::shared_ptr<Array> array;
  ArrayFromVector<TYPE, C_TYPE>(is_valid, values, &array);
  CheckArray(*array, options, expected, check_operator);
}

TEST_F(TestPrettyPrint, PrimitiveType) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  std::vector<int32_t> values = {0, 1, 2, 3, 4};
  static const char* expected = R"expected([
  0,
  1,
  null,
  3,
  null
])expected";
  CheckPrimitive<Int32Type, int32_t>({0, 10}, is_valid, values, expected);

  static const char* expected_na = R"expected([
  0,
  1,
  NA,
  3,
  NA
])expected";
  CheckPrimitive<Int32Type, int32_t>({0, 10, 2, "NA"}, is_valid, values, expected_na,
                                     false);

  static const char* ex_in2 = R"expected(  [
    0,
    1,
    null,
    3,
    null
  ])expected";
  CheckPrimitive<Int32Type, int32_t>({2, 10}, is_valid, values, ex_in2);
  static const char* ex_in2_w2 = R"expected(  [
    0,
    1,
    ...
    3,
    null
  ])expected";
  CheckPrimitive<Int32Type, int32_t>({2, 2}, is_valid, values, ex_in2_w2);

  std::vector<double> values2 = {0., 1., 2., 3., 4.};
  static const char* ex2 = R"expected([
  0,
  1,
  null,
  3,
  null
])expected";
  CheckPrimitive<DoubleType, double>({0, 10}, is_valid, values2, ex2);
  static const char* ex2_in2 = R"expected(  [
    0,
    1,
    null,
    3,
    null
  ])expected";
  CheckPrimitive<DoubleType, double>({2, 10}, is_valid, values2, ex2_in2);

  std::vector<std::string> values3 = {"foo", "bar", "", "baz", ""};
  static const char* ex3 = R"expected([
  "foo",
  "bar",
  null,
  "baz",
  null
])expected";
  CheckPrimitive<StringType, std::string>({0, 10}, is_valid, values3, ex3);
  static const char* ex3_in2 = R"expected(  [
    "foo",
    "bar",
    null,
    "baz",
    null
  ])expected";
  CheckPrimitive<StringType, std::string>({2, 10}, is_valid, values3, ex3_in2);
}

TEST_F(TestPrettyPrint, StructTypeBasic) {
  auto simple_1 = field("one", int32());
  auto simple_2 = field("two", int32());
  auto simple_struct = struct_({simple_1, simple_2});

  auto array = ArrayFromJSON(simple_struct, "[[11, 22]]");

  static const char* ex = R"expected(-- is_valid: all not null
-- child 0 type: int32
  [
    11
  ]
-- child 1 type: int32
  [
    22
  ])expected";
  CheckStream(*array, {0, 10}, ex);

  static const char* ex_2 = R"expected(  -- is_valid: all not null
  -- child 0 type: int32
    [
      11
    ]
  -- child 1 type: int32
    [
      22
    ])expected";
  CheckStream(*array, {2, 10}, ex_2);
}

TEST_F(TestPrettyPrint, StructTypeAdvanced) {
  auto simple_1 = field("one", int32());
  auto simple_2 = field("two", int32());
  auto simple_struct = struct_({simple_1, simple_2});

  auto array = ArrayFromJSON(simple_struct, "[[11, 22], null, [null, 33]]");

  static const char* ex = R"expected(-- is_valid:
  [
    true,
    false,
    true
  ]
-- child 0 type: int32
  [
    11,
    null,
    null
  ]
-- child 1 type: int32
  [
    22,
    null,
    33
  ])expected";
  CheckStream(*array, {0, 10}, ex);
}

TEST_F(TestPrettyPrint, BinaryType) {
  std::vector<bool> is_valid = {true, true, false, true, false};
  std::vector<std::string> values = {"foo", "bar", "", "baz", ""};
  static const char* ex = "[\n  666F6F,\n  626172,\n  null,\n  62617A,\n  null\n]";
  CheckPrimitive<BinaryType, std::string>({0}, is_valid, values, ex);
  static const char* ex_in2 =
      "  [\n    666F6F,\n    626172,\n    null,\n    62617A,\n    null\n  ]";
  CheckPrimitive<BinaryType, std::string>({2}, is_valid, values, ex_in2);
}

TEST_F(TestPrettyPrint, ListType) {
  auto list_type = list(int64());
  auto array = ArrayFromJSON(list_type, "[[null], [], null, [4, 6, 7], [2, 3]]");

  static const char* ex = R"expected([
  [
    null
  ],
  [],
  null,
  [
    4,
    6,
    7
  ],
  [
    2,
    3
  ]
])expected";
  CheckArray(*array, {0, 10}, ex);
  static const char* ex_2 = R"expected(  [
    [
      null
    ],
    [],
    null,
    [
      4,
      6,
      7
    ],
    [
      2,
      3
    ]
  ])expected";
  CheckArray(*array, {2, 10}, ex_2);
  static const char* ex_3 = R"expected([
  [
    null
  ],
  ...
  [
    2,
    3
  ]
])expected";
  CheckStream(*array, {0, 1}, ex_3);
}

TEST_F(TestPrettyPrint, FixedSizeBinaryType) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  auto type = fixed_size_binary(3);
  auto array = ArrayFromJSON(type, "[\"foo\", \"bar\", null, \"baz\"]");

  static const char* ex = "[\n  666F6F,\n  626172,\n  null,\n  62617A\n]";
  CheckArray(*array, {0, 10}, ex);
  static const char* ex_2 = "  [\n    666F6F,\n    ...\n    62617A\n  ]";
  CheckArray(*array, {2, 1}, ex_2);
}

TEST_F(TestPrettyPrint, Decimal128Type) {
  int32_t p = 19;
  int32_t s = 4;

  auto type = decimal(p, s);
  auto array = ArrayFromJSON(type, "[\"123.4567\", \"456.7891\", null]");

  static const char* ex = "[\n  123.4567,\n  456.7891,\n  null\n]";
  CheckArray(*array, {0}, ex);
}

TEST_F(TestPrettyPrint, DictionaryType) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  std::vector<std::string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, std::string>(dict_values, &dict);
  std::shared_ptr<DataType> dict_type = dictionary(int16(), dict);

  std::shared_ptr<Array> indices;
  std::vector<int16_t> indices_values = {1, 2, -1, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(is_valid, indices_values, &indices);
  auto arr = std::make_shared<DictionaryArray>(dict_type, indices);

  static const char* expected = R"expected(
-- dictionary:
  [
    "foo",
    "bar",
    "baz"
  ]
-- indices:
  [
    1,
    2,
    null,
    0,
    2,
    0
  ])expected";

  CheckArray(*arr, {0}, expected);
}

TEST_F(TestPrettyPrint, ChunkedArrayPrimitiveType) {
  auto array = ArrayFromJSON(int32(), "[0, 1, null, 3, null]");
  ChunkedArray chunked_array(array);

  static const char* expected = R"expected([
  [
    0,
    1,
    null,
    3,
    null
  ]
])expected";
  CheckStream(chunked_array, {0}, expected);

  ChunkedArray chunked_array_2({array, array});

  static const char* expected_2 = R"expected([
  [
    0,
    1,
    null,
    3,
    null
  ],
  [
    0,
    1,
    null,
    3,
    null
  ]
])expected";

  CheckStream(chunked_array_2, {0}, expected_2);
}

TEST_F(TestPrettyPrint, ColumnPrimitiveType) {
  std::shared_ptr<Field> int_field = field("column", int32());
  auto array = ArrayFromJSON(int_field->type(), "[0, 1, null, 3, null]");
  Column column(int_field, ArrayVector({array}));

  static const char* expected = R"expected(column: int32
[
  [
    0,
    1,
    null,
    3,
    null
  ]
])expected";
  CheckStream(column, {0}, expected);

  Column column_2(int_field, {array, array});

  static const char* expected_2 = R"expected(column: int32
[
  [
    0,
    1,
    null,
    3,
    null
  ],
  [
    0,
    1,
    null,
    3,
    null
  ]
])expected";

  CheckStream(column_2, {0}, expected_2);
}

TEST_F(TestPrettyPrint, TablePrimitive) {
  std::shared_ptr<Field> int_field = field("column", int32());
  auto array = ArrayFromJSON(int_field->type(), "[0, 1, null, 3, null]");
  std::shared_ptr<Column> column =
      std::make_shared<Column>(int_field, ArrayVector({array}));
  std::shared_ptr<Schema> table_schema = schema({int_field});
  std::shared_ptr<Table> table = Table::Make(table_schema, {column});

  static const char* expected = R"expected(column: int32
----
column:
  [
    [
      0,
      1,
      null,
      3,
      null
    ]
  ]
)expected";
  CheckStream(*table, {0}, expected);
}

TEST_F(TestPrettyPrint, SchemaWithDictionary) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  std::vector<std::string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, std::string>(dict_values, &dict);

  auto simple = field("one", int32());
  auto simple_dict = field("two", dictionary(int16(), dict));
  auto list_of_dict = field("three", list(simple_dict));

  auto struct_with_dict = field("four", struct_({simple, simple_dict}));

  auto sch = schema({simple, simple_dict, list_of_dict, struct_with_dict});

  static const char* expected = R"expected(one: int32
two: dictionary<values=string, indices=int16, ordered=0>
  dictionary:
    [
      "foo",
      "bar",
      "baz"
    ]
three: list<two: dictionary<values=string, indices=int16, ordered=0>>
  child 0, two: dictionary<values=string, indices=int16, ordered=0>
    dictionary:
      [
        "foo",
        "bar",
        "baz"
      ]
four: struct<one: int32, two: dictionary<values=string, indices=int16, ordered=0>>
  child 0, one: int32
  child 1, two: dictionary<values=string, indices=int16, ordered=0>
    dictionary:
      [
        "foo",
        "bar",
        "baz"
      ])expected";

  PrettyPrintOptions options{0};

  Check(*sch, options, expected);
}

}  // namespace arrow
