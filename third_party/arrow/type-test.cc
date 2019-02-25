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

// Unit tests for DataType (and subclasses), Field, and Schema

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"

using std::shared_ptr;
using std::vector;

namespace arrow {

using internal::checked_cast;

TEST(TestField, Basics) {
  Field f0("f0", int32());
  Field f0_nn("f0", int32(), false);

  ASSERT_EQ(f0.name(), "f0");
  ASSERT_EQ(f0.type()->ToString(), int32()->ToString());

  ASSERT_TRUE(f0.nullable());
  ASSERT_FALSE(f0_nn.nullable());
}

TEST(TestField, Equals) {
  auto meta = key_value_metadata({{"a", "1"}, {"b", "2"}});

  Field f0("f0", int32());
  Field f0_nn("f0", int32(), false);
  Field f0_other("f0", int32());
  Field f0_with_meta("f0", int32(), true, meta);

  ASSERT_TRUE(f0.Equals(f0_other));
  ASSERT_FALSE(f0.Equals(f0_nn));
  ASSERT_FALSE(f0.Equals(f0_with_meta));
  ASSERT_TRUE(f0.Equals(f0_with_meta, false));
}

TEST(TestField, TestMetadataConstruction) {
  auto metadata = std::shared_ptr<KeyValueMetadata>(
      new KeyValueMetadata({"foo", "bar"}, {"bizz", "buzz"}));
  auto metadata2 = metadata->Copy();
  auto f0 = field("f0", int32(), true, metadata);
  auto f1 = field("f0", int32(), true, metadata2);
  ASSERT_TRUE(metadata->Equals(*f0->metadata()));
  ASSERT_TRUE(f0->Equals(*f1));
}

TEST(TestField, TestAddMetadata) {
  auto metadata = std::shared_ptr<KeyValueMetadata>(
      new KeyValueMetadata({"foo", "bar"}, {"bizz", "buzz"}));
  auto f0 = field("f0", int32());
  auto f1 = field("f0", int32(), true, metadata);
  std::shared_ptr<Field> f2 = f0->AddMetadata(metadata);

  ASSERT_FALSE(f2->Equals(*f0));
  ASSERT_TRUE(f2->Equals(*f1));

  // Not copied
  ASSERT_TRUE(metadata.get() == f1->metadata().get());
}

TEST(TestField, TestRemoveMetadata) {
  auto metadata = std::shared_ptr<KeyValueMetadata>(
      new KeyValueMetadata({"foo", "bar"}, {"bizz", "buzz"}));
  auto f0 = field("f0", int32());
  auto f1 = field("f0", int32(), true, metadata);
  std::shared_ptr<Field> f2 = f1->RemoveMetadata();
  ASSERT_TRUE(f2->metadata() == nullptr);
}

TEST(TestField, TestFlatten) {
  auto metadata = std::shared_ptr<KeyValueMetadata>(
      new KeyValueMetadata({"foo", "bar"}, {"bizz", "buzz"}));
  auto f0 = field("f0", int32(), true /* nullable */, metadata);
  auto vec = f0->Flatten();
  ASSERT_EQ(vec.size(), 1);
  ASSERT_TRUE(vec[0]->Equals(*f0));

  auto f1 = field("f1", float64(), false /* nullable */);
  auto ff = field("nest", struct_({f0, f1}));
  vec = ff->Flatten();
  ASSERT_EQ(vec.size(), 2);
  auto expected0 = field("nest.f0", int32(), true /* nullable */, metadata);
  // nullable parent implies nullable flattened child
  auto expected1 = field("nest.f1", float64(), true /* nullable */);
  ASSERT_TRUE(vec[0]->Equals(*expected0));
  ASSERT_TRUE(vec[1]->Equals(*expected1));

  ff = field("nest", struct_({f0, f1}), false /* nullable */);
  vec = ff->Flatten();
  ASSERT_EQ(vec.size(), 2);
  expected0 = field("nest.f0", int32(), true /* nullable */, metadata);
  expected1 = field("nest.f1", float64(), false /* nullable */);
  ASSERT_TRUE(vec[0]->Equals(*expected0));
  ASSERT_TRUE(vec[1]->Equals(*expected1));
}

class TestSchema : public ::testing::Test {
 public:
  void SetUp() {}
};

TEST_F(TestSchema, Basics) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f1_optional = field("f1", uint8());

  auto f2 = field("f2", utf8());

  auto schema = ::arrow::schema({f0, f1, f2});

  ASSERT_EQ(3, schema->num_fields());
  ASSERT_TRUE(f0->Equals(schema->field(0)));
  ASSERT_TRUE(f1->Equals(schema->field(1)));
  ASSERT_TRUE(f2->Equals(schema->field(2)));

  auto schema2 = ::arrow::schema({f0, f1, f2});

  vector<shared_ptr<Field>> fields3 = {f0, f1_optional, f2};
  auto schema3 = std::make_shared<Schema>(fields3);
  ASSERT_TRUE(schema->Equals(*schema2));
  ASSERT_FALSE(schema->Equals(*schema3));
}

TEST_F(TestSchema, ToString) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  auto schema = ::arrow::schema({f0, f1, f2, f3});

  std::string result = schema->ToString();
  std::string expected = R"(f0: int32
f1: uint8 not null
f2: string
f3: list<item: int16>)";

  ASSERT_EQ(expected, result);
}

TEST_F(TestSchema, GetFieldByName) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  auto schema = ::arrow::schema({f0, f1, f2, f3});

  std::shared_ptr<Field> result;

  result = schema->GetFieldByName("f1");
  ASSERT_TRUE(f1->Equals(result));

  result = schema->GetFieldByName("f3");
  ASSERT_TRUE(f3->Equals(result));

  result = schema->GetFieldByName("not-found");
  ASSERT_TRUE(result == nullptr);
}

TEST_F(TestSchema, GetFieldIndex) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  auto schema = ::arrow::schema({f0, f1, f2, f3});

  ASSERT_EQ(0, schema->GetFieldIndex(f0->name()));
  ASSERT_EQ(1, schema->GetFieldIndex(f1->name()));
  ASSERT_EQ(2, schema->GetFieldIndex(f2->name()));
  ASSERT_EQ(3, schema->GetFieldIndex(f3->name()));
  ASSERT_EQ(-1, schema->GetFieldIndex("not-found"));
}

TEST_F(TestSchema, GetFieldDuplicates) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f1", list(int16()));

  auto schema = ::arrow::schema({f0, f1, f2, f3});

  ASSERT_EQ(0, schema->GetFieldIndex(f0->name()));
  ASSERT_EQ(-1, schema->GetFieldIndex(f1->name()));  // duplicate
  ASSERT_EQ(2, schema->GetFieldIndex(f2->name()));
  ASSERT_EQ(-1, schema->GetFieldIndex("not-found"));
  ASSERT_EQ(std::vector<int>{0}, schema->GetAllFieldIndices(f0->name()));
  AssertSortedEquals(std::vector<int>{1, 3}, schema->GetAllFieldIndices(f1->name()));

  std::vector<std::shared_ptr<Field>> results;

  results = schema->GetAllFieldsByName(f0->name());
  ASSERT_EQ(results.size(), 1);
  ASSERT_TRUE(results[0]->Equals(f0));

  results = schema->GetAllFieldsByName(f1->name());
  ASSERT_EQ(results.size(), 2);
  if (results[0]->type()->id() == Type::UINT8) {
    ASSERT_TRUE(results[0]->Equals(f1));
    ASSERT_TRUE(results[1]->Equals(f3));
  } else {
    ASSERT_TRUE(results[0]->Equals(f3));
    ASSERT_TRUE(results[1]->Equals(f1));
  }

  results = schema->GetAllFieldsByName("not-found");
  ASSERT_EQ(results.size(), 0);
}

TEST_F(TestSchema, TestMetadataConstruction) {
  auto metadata0 = key_value_metadata({{"foo", "bar"}, {"bizz", "buzz"}});
  auto metadata1 = key_value_metadata({{"foo", "baz"}});

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8(), true);
  auto f3 = field("f2", utf8(), true, metadata1->Copy());

  auto schema0 = ::arrow::schema({f0, f1, f2}, metadata0);
  auto schema1 = ::arrow::schema({f0, f1, f2}, metadata1);
  auto schema2 = ::arrow::schema({f0, f1, f2}, metadata0->Copy());
  auto schema3 = ::arrow::schema({f0, f1, f3}, metadata0->Copy());

  ASSERT_TRUE(metadata0->Equals(*schema0->metadata()));
  ASSERT_TRUE(metadata1->Equals(*schema1->metadata()));
  ASSERT_TRUE(metadata0->Equals(*schema2->metadata()));
  ASSERT_TRUE(schema0->Equals(*schema2));
  ASSERT_FALSE(schema0->Equals(*schema1));
  ASSERT_FALSE(schema2->Equals(*schema1));
  ASSERT_FALSE(schema2->Equals(*schema3));

  // don't check metadata
  ASSERT_TRUE(schema0->Equals(*schema1, false));
  ASSERT_TRUE(schema2->Equals(*schema1, false));
  ASSERT_TRUE(schema2->Equals(*schema3, false));
}

TEST_F(TestSchema, TestAddMetadata) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  vector<shared_ptr<Field>> fields = {f0, f1, f2};
  auto metadata = std::shared_ptr<KeyValueMetadata>(
      new KeyValueMetadata({"foo", "bar"}, {"bizz", "buzz"}));
  auto schema = std::make_shared<Schema>(fields);
  std::shared_ptr<Schema> new_schema = schema->AddMetadata(metadata);
  ASSERT_TRUE(metadata->Equals(*new_schema->metadata()));

  // Not copied
  ASSERT_TRUE(metadata.get() == new_schema->metadata().get());
}

TEST_F(TestSchema, TestRemoveMetadata) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  vector<shared_ptr<Field>> fields = {f0, f1, f2};
  KeyValueMetadata metadata({"foo", "bar"}, {"bizz", "buzz"});
  auto schema = std::make_shared<Schema>(fields);
  std::shared_ptr<Schema> new_schema = schema->RemoveMetadata();
  ASSERT_TRUE(new_schema->metadata() == nullptr);
}

#define PRIMITIVE_TEST(KLASS, CTYPE, ENUM, NAME)                              \
  TEST(TypesTest, ARROW_CONCAT(TestPrimitive_, ENUM)) {                       \
    KLASS tp;                                                                 \
                                                                              \
    ASSERT_EQ(tp.id(), Type::ENUM);                                           \
    ASSERT_EQ(tp.ToString(), std::string(NAME));                              \
                                                                              \
    using CType = TypeTraits<KLASS>::CType;                                   \
    static_assert(std::is_same<CType, CTYPE>::value, "Not the same c-type!"); \
                                                                              \
    using DerivedArrowType = CTypeTraits<CTYPE>::ArrowType;                   \
    static_assert(std::is_same<DerivedArrowType, KLASS>::value,               \
                  "Not the same arrow-type!");                                \
  }

PRIMITIVE_TEST(Int8Type, int8_t, INT8, "int8");
PRIMITIVE_TEST(Int16Type, int16_t, INT16, "int16");
PRIMITIVE_TEST(Int32Type, int32_t, INT32, "int32");
PRIMITIVE_TEST(Int64Type, int64_t, INT64, "int64");
PRIMITIVE_TEST(UInt8Type, uint8_t, UINT8, "uint8");
PRIMITIVE_TEST(UInt16Type, uint16_t, UINT16, "uint16");
PRIMITIVE_TEST(UInt32Type, uint32_t, UINT32, "uint32");
PRIMITIVE_TEST(UInt64Type, uint64_t, UINT64, "uint64");

PRIMITIVE_TEST(FloatType, float, FLOAT, "float");
PRIMITIVE_TEST(DoubleType, double, DOUBLE, "double");

PRIMITIVE_TEST(BooleanType, bool, BOOL, "bool");

TEST(TestBinaryType, ToString) {
  BinaryType t1;
  BinaryType e1;
  StringType t2;
  EXPECT_TRUE(t1.Equals(e1));
  EXPECT_FALSE(t1.Equals(t2));
  ASSERT_EQ(t1.id(), Type::BINARY);
  ASSERT_EQ(t1.ToString(), std::string("binary"));
}

TEST(TestStringType, ToString) {
  StringType str;
  ASSERT_EQ(str.id(), Type::STRING);
  ASSERT_EQ(str.ToString(), std::string("string"));
}

TEST(TestFixedSizeBinaryType, ToString) {
  auto t = fixed_size_binary(10);
  ASSERT_EQ(t->id(), Type::FIXED_SIZE_BINARY);
  ASSERT_EQ("fixed_size_binary[10]", t->ToString());
}

TEST(TestFixedSizeBinaryType, Equals) {
  auto t1 = fixed_size_binary(10);
  auto t2 = fixed_size_binary(10);
  auto t3 = fixed_size_binary(3);

  ASSERT_TRUE(t1->Equals(t1));
  ASSERT_TRUE(t1->Equals(t2));
  ASSERT_FALSE(t1->Equals(t3));
}

TEST(TestListType, Basics) {
  std::shared_ptr<DataType> vt = std::make_shared<UInt8Type>();

  ListType list_type(vt);
  ASSERT_EQ(list_type.id(), Type::LIST);

  ASSERT_EQ("list", list_type.name());
  ASSERT_EQ("list<item: uint8>", list_type.ToString());

  ASSERT_EQ(list_type.value_type()->id(), vt->id());
  ASSERT_EQ(list_type.value_type()->id(), vt->id());

  std::shared_ptr<DataType> st = std::make_shared<StringType>();
  std::shared_ptr<DataType> lt = std::make_shared<ListType>(st);
  ASSERT_EQ("list<item: string>", lt->ToString());

  ListType lt2(lt);
  ASSERT_EQ("list<item: list<item: string>>", lt2.ToString());
}

TEST(TestDateTypes, Attrs) {
  auto t1 = date32();
  auto t2 = date64();

  ASSERT_EQ("date32[day]", t1->ToString());
  ASSERT_EQ("date64[ms]", t2->ToString());

  ASSERT_EQ(32, checked_cast<const FixedWidthType&>(*t1).bit_width());
  ASSERT_EQ(64, checked_cast<const FixedWidthType&>(*t2).bit_width());
}

TEST(TestTimeType, Equals) {
  Time32Type t0;
  Time32Type t1(TimeUnit::SECOND);
  Time32Type t2(TimeUnit::MILLI);
  Time64Type t3(TimeUnit::MICRO);
  Time64Type t4(TimeUnit::NANO);
  Time64Type t5(TimeUnit::MICRO);

  ASSERT_EQ(32, t0.bit_width());
  ASSERT_EQ(64, t3.bit_width());

  ASSERT_TRUE(t0.Equals(t2));
  ASSERT_TRUE(t1.Equals(t1));
  ASSERT_FALSE(t1.Equals(t3));
  ASSERT_FALSE(t3.Equals(t4));
  ASSERT_TRUE(t3.Equals(t5));
}

TEST(TestTimeType, ToString) {
  auto t1 = time32(TimeUnit::MILLI);
  auto t2 = time64(TimeUnit::NANO);
  auto t3 = time32(TimeUnit::SECOND);
  auto t4 = time64(TimeUnit::MICRO);

  ASSERT_EQ("time32[ms]", t1->ToString());
  ASSERT_EQ("time64[ns]", t2->ToString());
  ASSERT_EQ("time32[s]", t3->ToString());
  ASSERT_EQ("time64[us]", t4->ToString());
}

TEST(TestTimestampType, Equals) {
  TimestampType t1;
  TimestampType t2;
  TimestampType t3(TimeUnit::NANO);
  TimestampType t4(TimeUnit::NANO);

  ASSERT_TRUE(t1.Equals(t2));
  ASSERT_FALSE(t1.Equals(t3));
  ASSERT_TRUE(t3.Equals(t4));
}

TEST(TestTimestampType, ToString) {
  auto t1 = timestamp(TimeUnit::MILLI);
  auto t2 = timestamp(TimeUnit::NANO, "US/Eastern");
  auto t3 = timestamp(TimeUnit::SECOND);
  auto t4 = timestamp(TimeUnit::MICRO);

  ASSERT_EQ("timestamp[ms]", t1->ToString());
  ASSERT_EQ("timestamp[ns, tz=US/Eastern]", t2->ToString());
  ASSERT_EQ("timestamp[s]", t3->ToString());
  ASSERT_EQ("timestamp[us]", t4->ToString());
}

TEST(TestNestedType, Equals) {
  auto create_struct = [](std::string inner_name,
                          std::string struct_name) -> shared_ptr<Field> {
    auto f_type = field(inner_name, int32());
    vector<shared_ptr<Field>> fields = {f_type};
    auto s_type = std::make_shared<StructType>(fields);
    return field(struct_name, s_type);
  };

  auto create_union = [](std::string inner_name,
                         std::string union_name) -> shared_ptr<Field> {
    auto f_type = field(inner_name, int32());
    vector<shared_ptr<Field>> fields = {f_type};
    vector<uint8_t> codes = {Type::INT32};
    auto u_type = std::make_shared<UnionType>(fields, codes, UnionMode::SPARSE);
    return field(union_name, u_type);
  };

  auto s0 = create_struct("f0", "s0");
  auto s0_other = create_struct("f0", "s0");
  auto s0_bad = create_struct("f1", "s0");
  auto s1 = create_struct("f1", "s1");

  ASSERT_TRUE(s0->Equals(s0_other));
  ASSERT_FALSE(s0->Equals(s1));
  ASSERT_FALSE(s0->Equals(s0_bad));

  auto u0 = create_union("f0", "u0");
  auto u0_other = create_union("f0", "u0");
  auto u0_bad = create_union("f1", "u0");
  auto u1 = create_union("f1", "u1");

  ASSERT_TRUE(u0->Equals(u0_other));
  ASSERT_FALSE(u0->Equals(u1));
  ASSERT_FALSE(u0->Equals(u0_bad));
}

TEST(TestStructType, Basics) {
  auto f0_type = int32();
  auto f0 = field("f0", f0_type);

  auto f1_type = utf8();
  auto f1 = field("f1", f1_type);

  auto f2_type = uint8();
  auto f2 = field("f2", f2_type);

  vector<std::shared_ptr<Field>> fields = {f0, f1, f2};

  StructType struct_type(fields);

  ASSERT_TRUE(struct_type.child(0)->Equals(f0));
  ASSERT_TRUE(struct_type.child(1)->Equals(f1));
  ASSERT_TRUE(struct_type.child(2)->Equals(f2));

  ASSERT_EQ(struct_type.ToString(), "struct<f0: int32, f1: string, f2: uint8>");

  // TODO(wesm): out of bounds for field(...)
}

TEST(TestStructType, GetFieldByName) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  StructType struct_type({f0, f1, f2, f3});
  std::shared_ptr<Field> result;

  result = struct_type.GetFieldByName("f1");
  ASSERT_EQ(f1, result);

  result = struct_type.GetFieldByName("f3");
  ASSERT_EQ(f3, result);

  result = struct_type.GetFieldByName("not-found");
  ASSERT_EQ(result, nullptr);
}

TEST(TestStructType, GetFieldIndex) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  StructType struct_type({f0, f1, f2, f3});

  ASSERT_EQ(0, struct_type.GetFieldIndex(f0->name()));
  ASSERT_EQ(1, struct_type.GetFieldIndex(f1->name()));
  ASSERT_EQ(2, struct_type.GetFieldIndex(f2->name()));
  ASSERT_EQ(3, struct_type.GetFieldIndex(f3->name()));
  ASSERT_EQ(-1, struct_type.GetFieldIndex("not-found"));
}

TEST(TestStructType, GetFieldDuplicates) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", int64());
  auto f2 = field("f1", utf8());
  StructType struct_type({f0, f1, f2});

  ASSERT_EQ(0, struct_type.GetFieldIndex("f0"));
  ASSERT_EQ(-1, struct_type.GetFieldIndex("f1"));
  ASSERT_EQ(std::vector<int>{0}, struct_type.GetAllFieldIndices(f0->name()));
  AssertSortedEquals(std::vector<int>{1, 2}, struct_type.GetAllFieldIndices(f1->name()));

  std::vector<std::shared_ptr<Field>> results;

  results = struct_type.GetAllFieldsByName(f0->name());
  ASSERT_EQ(results.size(), 1);
  ASSERT_TRUE(results[0]->Equals(f0));

  results = struct_type.GetAllFieldsByName(f1->name());
  ASSERT_EQ(results.size(), 2);
  if (results[0]->type()->id() == Type::INT64) {
    ASSERT_TRUE(results[0]->Equals(f1));
    ASSERT_TRUE(results[1]->Equals(f2));
  } else {
    ASSERT_TRUE(results[0]->Equals(f2));
    ASSERT_TRUE(results[1]->Equals(f1));
  }

  results = struct_type.GetAllFieldsByName("not-found");
  ASSERT_EQ(results.size(), 0);
}

TEST(TestDictionaryType, Equals) {
  auto t1 = dictionary(int8(), ArrayFromJSON(int32(), "[3, 4, 5, 6]"));
  auto t2 = dictionary(int8(), ArrayFromJSON(int32(), "[3, 4, 5, 6]"));
  auto t3 = dictionary(int16(), ArrayFromJSON(int32(), "[3, 4, 5, 6]"));
  auto t4 = dictionary(int8(), ArrayFromJSON(int16(), "[3, 4, 5, 6]"));
  auto t5 = dictionary(int8(), ArrayFromJSON(int32(), "[3, 4, 7, 6]"));

  ASSERT_TRUE(t1->Equals(t2));
  // Different index type
  ASSERT_FALSE(t1->Equals(t3));
  // Different value type
  ASSERT_FALSE(t1->Equals(t4));
  // Different values
  ASSERT_FALSE(t1->Equals(t5));
}

TEST(TestDictionaryType, UnifyNumeric) {
  auto t1 = dictionary(int8(), ArrayFromJSON(int64(), "[3, 4, 7]"));
  auto t2 = dictionary(int8(), ArrayFromJSON(int64(), "[1, 7, 4, 8]"));
  auto t3 = dictionary(int8(), ArrayFromJSON(int64(), "[1, -200]"));

  auto expected = dictionary(int8(), ArrayFromJSON(int64(), "[3, 4, 7, 1, 8, -200]"));

  std::shared_ptr<DataType> dict_type;
  ASSERT_OK(DictionaryType::Unify(default_memory_pool(), {t1.get(), t2.get(), t3.get()},
                                  &dict_type));
  ASSERT_TRUE(dict_type->Equals(expected));

  std::vector<std::vector<int32_t>> transpose_maps;
  ASSERT_OK(DictionaryType::Unify(default_memory_pool(), {t1.get(), t2.get(), t3.get()},
                                  &dict_type, &transpose_maps));
  ASSERT_TRUE(dict_type->Equals(expected));
  ASSERT_EQ(transpose_maps.size(), 3);
  ASSERT_EQ(transpose_maps[0], std::vector<int32_t>({0, 1, 2}));
  ASSERT_EQ(transpose_maps[1], std::vector<int32_t>({3, 2, 1, 4}));
  ASSERT_EQ(transpose_maps[2], std::vector<int32_t>({3, 5}));
}

TEST(TestDictionaryType, UnifyString) {
  auto t1 = dictionary(int16(), ArrayFromJSON(utf8(), "[\"foo\", \"bar\"]"));
  auto t2 = dictionary(int32(), ArrayFromJSON(utf8(), "[\"quux\", \"foo\"]"));

  auto expected =
      dictionary(int8(), ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"quux\"]"));

  std::shared_ptr<DataType> dict_type;
  ASSERT_OK(
      DictionaryType::Unify(default_memory_pool(), {t1.get(), t2.get()}, &dict_type));
  ASSERT_TRUE(dict_type->Equals(expected));

  std::vector<std::vector<int32_t>> transpose_maps;
  ASSERT_OK(DictionaryType::Unify(default_memory_pool(), {t1.get(), t2.get()}, &dict_type,
                                  &transpose_maps));
  ASSERT_TRUE(dict_type->Equals(expected));

  ASSERT_EQ(transpose_maps.size(), 2);
  ASSERT_EQ(transpose_maps[0], std::vector<int32_t>({0, 1}));
  ASSERT_EQ(transpose_maps[1], std::vector<int32_t>({2, 0}));
}

TEST(TestDictionaryType, UnifyFixedSizeBinary) {
  auto type = fixed_size_binary(3);

  std::string data = "foobarbazqux";
  auto buf = std::make_shared<Buffer>(data);
  // ["foo", "bar"]
  auto dict1 = std::make_shared<FixedSizeBinaryArray>(type, 2, SliceBuffer(buf, 0, 6));
  auto t1 = dictionary(int16(), dict1);
  // ["bar", "baz", "qux"]
  auto dict2 = std::make_shared<FixedSizeBinaryArray>(type, 3, SliceBuffer(buf, 3, 9));
  auto t2 = dictionary(int16(), dict2);

  // ["foo", "bar", "baz", "qux"]
  auto expected_dict = std::make_shared<FixedSizeBinaryArray>(type, 4, buf);
  auto expected = dictionary(int8(), expected_dict);

  std::shared_ptr<DataType> dict_type;
  ASSERT_OK(
      DictionaryType::Unify(default_memory_pool(), {t1.get(), t2.get()}, &dict_type));
  ASSERT_TRUE(dict_type->Equals(expected));

  std::vector<std::vector<int32_t>> transpose_maps;
  ASSERT_OK(DictionaryType::Unify(default_memory_pool(), {t1.get(), t2.get()}, &dict_type,
                                  &transpose_maps));
  ASSERT_TRUE(dict_type->Equals(expected));
  ASSERT_EQ(transpose_maps.size(), 2);
  ASSERT_EQ(transpose_maps[0], std::vector<int32_t>({0, 1}));
  ASSERT_EQ(transpose_maps[1], std::vector<int32_t>({1, 2, 3}));
}

TEST(TestDictionaryType, UnifyLarge) {
  // Unifying "large" dictionary types should choose the right index type
  std::shared_ptr<Array> dict1, dict2, expected_dict;

  Int32Builder builder;
  ASSERT_OK(builder.Reserve(120));
  for (int32_t i = 0; i < 120; ++i) {
    builder.UnsafeAppend(i);
  }
  ASSERT_OK(builder.Finish(&dict1));
  ASSERT_EQ(dict1->length(), 120);
  auto t1 = dictionary(int8(), dict1);

  ASSERT_OK(builder.Reserve(30));
  for (int32_t i = 110; i < 140; ++i) {
    builder.UnsafeAppend(i);
  }
  ASSERT_OK(builder.Finish(&dict2));
  ASSERT_EQ(dict2->length(), 30);
  auto t2 = dictionary(int8(), dict2);

  ASSERT_OK(builder.Reserve(140));
  for (int32_t i = 0; i < 140; ++i) {
    builder.UnsafeAppend(i);
  }
  ASSERT_OK(builder.Finish(&expected_dict));
  ASSERT_EQ(expected_dict->length(), 140);
  // int8 would be too narrow to hold all possible index values
  auto expected = dictionary(int16(), expected_dict);

  std::shared_ptr<DataType> dict_type;
  ASSERT_OK(
      DictionaryType::Unify(default_memory_pool(), {t1.get(), t2.get()}, &dict_type));
  ASSERT_TRUE(dict_type->Equals(expected));
}

TEST(TypesTest, TestDecimal128Small) {
  Decimal128Type t1(8, 4);

  ASSERT_EQ(t1.id(), Type::DECIMAL);
  ASSERT_EQ(t1.precision(), 8);
  ASSERT_EQ(t1.scale(), 4);

  ASSERT_EQ(t1.ToString(), std::string("decimal(8, 4)"));

  // Test properties
  ASSERT_EQ(t1.byte_width(), 16);
  ASSERT_EQ(t1.bit_width(), 128);
}

TEST(TypesTest, TestDecimal128Medium) {
  Decimal128Type t1(12, 5);

  ASSERT_EQ(t1.id(), Type::DECIMAL);
  ASSERT_EQ(t1.precision(), 12);
  ASSERT_EQ(t1.scale(), 5);

  ASSERT_EQ(t1.ToString(), std::string("decimal(12, 5)"));

  // Test properties
  ASSERT_EQ(t1.byte_width(), 16);
  ASSERT_EQ(t1.bit_width(), 128);
}

TEST(TypesTest, TestDecimal128Large) {
  Decimal128Type t1(27, 7);

  ASSERT_EQ(t1.id(), Type::DECIMAL);
  ASSERT_EQ(t1.precision(), 27);
  ASSERT_EQ(t1.scale(), 7);

  ASSERT_EQ(t1.ToString(), std::string("decimal(27, 7)"));

  // Test properties
  ASSERT_EQ(t1.byte_width(), 16);
  ASSERT_EQ(t1.bit_width(), 128);
}

}  // namespace arrow
