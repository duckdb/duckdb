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
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/stl.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

using primitive_types_tuple = std::tuple<int8_t, int16_t, int32_t, int64_t, uint8_t,
                                         uint16_t, uint32_t, uint64_t, bool, std::string>;

namespace arrow {
namespace stl {

TEST(TestSchemaFromTuple, PrimitiveTypesVector) {
  Schema expected_schema(
      {field("column1", int8(), false), field("column2", int16(), false),
       field("column3", int32(), false), field("column4", int64(), false),
       field("column5", uint8(), false), field("column6", uint16(), false),
       field("column7", uint32(), false), field("column8", uint64(), false),
       field("column9", boolean(), false), field("column10", utf8(), false)});

  std::shared_ptr<Schema> schema = SchemaFromTuple<primitive_types_tuple>::MakeSchema(
      std::vector<std::string>({"column1", "column2", "column3", "column4", "column5",
                                "column6", "column7", "column8", "column9", "column10"}));
  ASSERT_TRUE(expected_schema.Equals(*schema));
}

TEST(TestSchemaFromTuple, PrimitiveTypesTuple) {
  Schema expected_schema(
      {field("column1", int8(), false), field("column2", int16(), false),
       field("column3", int32(), false), field("column4", int64(), false),
       field("column5", uint8(), false), field("column6", uint16(), false),
       field("column7", uint32(), false), field("column8", uint64(), false),
       field("column9", boolean(), false), field("column10", utf8(), false)});

  std::shared_ptr<Schema> schema = SchemaFromTuple<primitive_types_tuple>::MakeSchema(
      std::make_tuple("column1", "column2", "column3", "column4", "column5", "column6",
                      "column7", "column8", "column9", "column10"));
  ASSERT_TRUE(expected_schema.Equals(*schema));
}

TEST(TestSchemaFromTuple, SimpleList) {
  Schema expected_schema({field("column1", list(utf8()), false)});
  std::shared_ptr<Schema> schema =
      SchemaFromTuple<std::tuple<std::vector<std::string>>>::MakeSchema({"column1"});

  ASSERT_TRUE(expected_schema.Equals(*schema));
}

TEST(TestSchemaFromTuple, NestedList) {
  Schema expected_schema({field("column1", list(list(boolean())), false)});
  std::shared_ptr<Schema> schema =
      SchemaFromTuple<std::tuple<std::vector<std::vector<bool>>>>::MakeSchema(
          {"column1"});

  ASSERT_TRUE(expected_schema.Equals(*schema));
}

TEST(TestTableFromTupleVector, PrimitiveTypes) {
  std::vector<std::string> names{"column1", "column2", "column3", "column4", "column5",
                                 "column6", "column7", "column8", "column9", "column10"};
  std::vector<primitive_types_tuple> rows{
      primitive_types_tuple(-1, -2, -3, -4, 1, 2, 3, 4, true, "Tests"),
      primitive_types_tuple(-10, -20, -30, -40, 10, 20, 30, 40, false, "Other")};
  std::shared_ptr<Table> table;
  ASSERT_OK(TableFromTupleRange(default_memory_pool(), rows, names, &table));

  std::shared_ptr<Schema> expected_schema =
      schema({field("column1", int8(), false), field("column2", int16(), false),
              field("column3", int32(), false), field("column4", int64(), false),
              field("column5", uint8(), false), field("column6", uint16(), false),
              field("column7", uint32(), false), field("column8", uint64(), false),
              field("column9", boolean(), false), field("column10", utf8(), false)});

  // Construct expected arrays
  std::shared_ptr<Array> int8_array = ArrayFromJSON(int8(), "[-1, -10]");
  std::shared_ptr<Array> int16_array = ArrayFromJSON(int16(), "[-2, -20]");
  std::shared_ptr<Array> int32_array = ArrayFromJSON(int32(), "[-3, -30]");
  std::shared_ptr<Array> int64_array = ArrayFromJSON(int64(), "[-4, -40]");
  std::shared_ptr<Array> uint8_array = ArrayFromJSON(uint8(), "[1, 10]");
  std::shared_ptr<Array> uint16_array = ArrayFromJSON(uint16(), "[2, 20]");
  std::shared_ptr<Array> uint32_array = ArrayFromJSON(uint32(), "[3, 30]");
  std::shared_ptr<Array> uint64_array = ArrayFromJSON(uint64(), "[4, 40]");
  std::shared_ptr<Array> bool_array = ArrayFromJSON(boolean(), "[true, false]");
  std::shared_ptr<Array> string_array = ArrayFromJSON(utf8(), R"(["Tests", "Other"])");
  auto expected_table =
      Table::Make(expected_schema,
                  {int8_array, int16_array, int32_array, int64_array, uint8_array,
                   uint16_array, uint32_array, uint64_array, bool_array, string_array});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestTableFromTupleVector, ListType) {
  using tuple_type = std::tuple<std::vector<int64_t>>;

  auto expected_schema =
      std::shared_ptr<Schema>(new Schema({field("column1", list(int64()), false)}));
  std::shared_ptr<Array> expected_array =
      ArrayFromJSON(list(int64()), "[[1, 1, 2, 34], [2, -4]]");
  std::shared_ptr<Table> expected_table = Table::Make(expected_schema, {expected_array});

  std::vector<tuple_type> rows{tuple_type(std::vector<int64_t>{1, 1, 2, 34}),
                               tuple_type(std::vector<int64_t>{2, -4})};
  std::vector<std::string> names{"column1"};

  std::shared_ptr<Table> table;
  ASSERT_OK(TableFromTupleRange(default_memory_pool(), rows, names, &table));
  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestTupleVectorFromTable, PrimitiveTypes) {
  compute::FunctionContext ctx;
  compute::CastOptions cast_options;

  std::vector<primitive_types_tuple> expected_rows{
      primitive_types_tuple(-1, -2, -3, -4, 1, 2, 3, 4, true, "Tests"),
      primitive_types_tuple(-10, -20, -30, -40, 10, 20, 30, 40, false, "Other")};

  std::shared_ptr<Schema> schema = std::shared_ptr<Schema>(
      new Schema({field("column1", int8(), false), field("column2", int16(), false),
                  field("column3", int32(), false), field("column4", int64(), false),
                  field("column5", uint8(), false), field("column6", uint16(), false),
                  field("column7", uint32(), false), field("column8", uint64(), false),
                  field("column9", boolean(), false), field("column10", utf8(), false)}));

  // Construct expected arrays
  std::shared_ptr<Array> int8_array;
  ArrayFromVector<Int8Type, int8_t>({-1, -10}, &int8_array);
  std::shared_ptr<Array> int16_array;
  ArrayFromVector<Int16Type, int16_t>({-2, -20}, &int16_array);
  std::shared_ptr<Array> int32_array;
  ArrayFromVector<Int32Type, int32_t>({-3, -30}, &int32_array);
  std::shared_ptr<Array> int64_array;
  ArrayFromVector<Int64Type, int64_t>({-4, -40}, &int64_array);
  std::shared_ptr<Array> uint8_array;
  ArrayFromVector<UInt8Type, uint8_t>({1, 10}, &uint8_array);
  std::shared_ptr<Array> uint16_array;
  ArrayFromVector<UInt16Type, uint16_t>({2, 20}, &uint16_array);
  std::shared_ptr<Array> uint32_array;
  ArrayFromVector<UInt32Type, uint32_t>({3, 30}, &uint32_array);
  std::shared_ptr<Array> uint64_array;
  ArrayFromVector<UInt64Type, uint64_t>({4, 40}, &uint64_array);
  std::shared_ptr<Array> bool_array;
  ArrayFromVector<BooleanType, bool>({true, false}, &bool_array);
  std::shared_ptr<Array> string_array;
  ArrayFromVector<StringType, std::string>({"Tests", "Other"}, &string_array);
  auto table = Table::Make(
      schema, {int8_array, int16_array, int32_array, int64_array, uint8_array,
               uint16_array, uint32_array, uint64_array, bool_array, string_array});

  std::vector<primitive_types_tuple> rows(2);
  ASSERT_OK(TupleRangeFromTable(*table, cast_options, &ctx, &rows));
  ASSERT_EQ(rows, expected_rows);

  // The number of rows must match
  std::vector<primitive_types_tuple> too_few_rows(1);
  ASSERT_RAISES(Invalid, TupleRangeFromTable(*table, cast_options, &ctx, &too_few_rows));

  // The number of columns must match
  std::shared_ptr<Table> corrupt_table;
  ASSERT_OK(table->RemoveColumn(0, &corrupt_table));
  ASSERT_RAISES(Invalid, TupleRangeFromTable(*corrupt_table, cast_options, &ctx, &rows));
}

TEST(TestTupleVectorFromTable, ListType) {
  using tuple_type = std::tuple<std::vector<int64_t>>;

  compute::FunctionContext ctx;
  compute::CastOptions cast_options;
  auto expected_schema =
      std::shared_ptr<Schema>(new Schema({field("column1", list(int64()), false)}));
  std::shared_ptr<Array> expected_array =
      ArrayFromJSON(list(int64()), "[[1, 1, 2, 34], [2, -4]]");
  std::shared_ptr<Table> table = Table::Make(expected_schema, {expected_array});

  std::vector<tuple_type> expected_rows{tuple_type(std::vector<int64_t>{1, 1, 2, 34}),
                                        tuple_type(std::vector<int64_t>{2, -4})};

  std::vector<tuple_type> rows(2);
  ASSERT_OK(TupleRangeFromTable(*table, cast_options, &ctx, &rows));
  ASSERT_EQ(rows, expected_rows);
}

TEST(TestTupleVectorFromTable, CastingNeeded) {
  using tuple_type = std::tuple<std::vector<int64_t>>;

  compute::FunctionContext ctx;
  compute::CastOptions cast_options;
  auto expected_schema =
      std::shared_ptr<Schema>(new Schema({field("column1", list(int16()), false)}));
  std::shared_ptr<Array> expected_array =
      ArrayFromJSON(list(int16()), "[[1, 1, 2, 34], [2, -4]]");
  std::shared_ptr<Table> table = Table::Make(expected_schema, {expected_array});

  std::vector<tuple_type> expected_rows{tuple_type(std::vector<int64_t>{1, 1, 2, 34}),
                                        tuple_type(std::vector<int64_t>{2, -4})};

  std::vector<tuple_type> rows(2);
  ASSERT_OK(TupleRangeFromTable(*table, cast_options, &ctx, &rows));
  ASSERT_EQ(rows, expected_rows);
}

}  // namespace stl
}  // namespace arrow
