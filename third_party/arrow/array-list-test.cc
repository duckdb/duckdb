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
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using std::string;
using std::vector;

using internal::checked_cast;

// ----------------------------------------------------------------------
// List tests

class TestListArray : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();

    value_type_ = int32();
    type_ = list(value_type_);

    std::unique_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_.reset(checked_cast<ListBuilder*>(tmp.release()));
  }

  void Done() {
    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder_.get(), &out);
    result_ = std::dynamic_pointer_cast<ListArray>(out);
  }

 protected:
  std::shared_ptr<DataType> value_type_;

  std::shared_ptr<ListBuilder> builder_;
  std::shared_ptr<ListArray> result_;
};

TEST_F(TestListArray, Equality) {
  Int32Builder* vb = checked_cast<Int32Builder*>(builder_->value_builder());

  std::shared_ptr<Array> array, equal_array, unequal_array;
  vector<int32_t> equal_offsets = {0, 1, 2, 5, 6, 7, 8, 10};
  vector<int32_t> equal_values = {1, 2, 3, 4, 5, 2, 2, 2, 5, 6};
  vector<int32_t> unequal_offsets = {0, 1, 4, 7};
  vector<int32_t> unequal_values = {1, 2, 2, 2, 3, 4, 5};

  // setup two equal arrays
  ASSERT_OK(builder_->AppendValues(equal_offsets.data(), equal_offsets.size()));
  ASSERT_OK(vb->AppendValues(equal_values.data(), equal_values.size()));

  ASSERT_OK(builder_->Finish(&array));
  ASSERT_OK(builder_->AppendValues(equal_offsets.data(), equal_offsets.size()));
  ASSERT_OK(vb->AppendValues(equal_values.data(), equal_values.size()));

  ASSERT_OK(builder_->Finish(&equal_array));
  // now an unequal one
  ASSERT_OK(builder_->AppendValues(unequal_offsets.data(), unequal_offsets.size()));
  ASSERT_OK(vb->AppendValues(unequal_values.data(), unequal_values.size()));

  ASSERT_OK(builder_->Finish(&unequal_array));

  // Test array equality
  EXPECT_TRUE(array->Equals(array));
  EXPECT_TRUE(array->Equals(equal_array));
  EXPECT_TRUE(equal_array->Equals(array));
  EXPECT_FALSE(equal_array->Equals(unequal_array));
  EXPECT_FALSE(unequal_array->Equals(equal_array));

  // Test range equality
  EXPECT_TRUE(array->RangeEquals(0, 1, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(0, 2, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(1, 2, 1, unequal_array));
  EXPECT_TRUE(array->RangeEquals(2, 3, 2, unequal_array));

  // Check with slices, ARROW-33
  std::shared_ptr<Array> slice, slice2;

  slice = array->Slice(2);
  slice2 = array->Slice(2);
  ASSERT_EQ(array->length() - 2, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(2, slice->length(), 0, slice));

  // Chained slices
  slice2 = array->Slice(1)->Slice(1);
  ASSERT_TRUE(slice->Equals(slice2));

  slice = array->Slice(1, 4);
  slice2 = array->Slice(1, 4);
  ASSERT_EQ(4, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(1, 5, 0, slice));
}

TEST_F(TestListArray, TestResize) {}

TEST_F(TestListArray, TestFromArrays) {
  std::shared_ptr<Array> offsets1, offsets2, offsets3, offsets4, values;

  std::vector<bool> offsets_is_valid3 = {true, false, true, true};
  std::vector<bool> offsets_is_valid4 = {true, true, false, true};

  std::vector<bool> values_is_valid = {true, false, true, true, true, true};

  std::vector<int32_t> offset1_values = {0, 2, 2, 6};
  std::vector<int32_t> offset2_values = {0, 2, 6, 6};

  std::vector<int8_t> values_values = {0, 1, 2, 3, 4, 5};
  const int length = 3;

  ArrayFromVector<Int32Type, int32_t>(offset1_values, &offsets1);
  ArrayFromVector<Int32Type, int32_t>(offset2_values, &offsets2);

  ArrayFromVector<Int32Type, int32_t>(offsets_is_valid3, offset1_values, &offsets3);
  ArrayFromVector<Int32Type, int32_t>(offsets_is_valid4, offset2_values, &offsets4);

  ArrayFromVector<Int8Type, int8_t>(values_is_valid, values_values, &values);

  auto list_type = list(int8());

  std::shared_ptr<Array> list1, list3, list4;
  ASSERT_OK(ListArray::FromArrays(*offsets1, *values, pool_, &list1));
  ASSERT_OK(ListArray::FromArrays(*offsets3, *values, pool_, &list3));
  ASSERT_OK(ListArray::FromArrays(*offsets4, *values, pool_, &list4));

  ListArray expected1(list_type, length, offsets1->data()->buffers[1], values,
                      offsets1->data()->buffers[0], 0);
  AssertArraysEqual(expected1, *list1);

  // Use null bitmap from offsets3, but clean offsets from non-null version
  ListArray expected3(list_type, length, offsets1->data()->buffers[1], values,
                      offsets3->data()->buffers[0], 1);
  AssertArraysEqual(expected3, *list3);

  // Check that the last offset bit is zero
  ASSERT_FALSE(BitUtil::GetBit(list3->null_bitmap()->data(), length + 1));

  ListArray expected4(list_type, length, offsets2->data()->buffers[1], values,
                      offsets4->data()->buffers[0], 1);
  AssertArraysEqual(expected4, *list4);

  // Test failure modes

  std::shared_ptr<Array> tmp;

  // Zero-length offsets
  ASSERT_RAISES(Invalid,
                ListArray::FromArrays(*offsets1->Slice(0, 0), *values, pool_, &tmp));

  // Offsets not int32
  ASSERT_RAISES(Invalid, ListArray::FromArrays(*values, *offsets1, pool_, &tmp));
}

TEST_F(TestListArray, TestAppendNull) {
  ASSERT_OK(builder_->AppendNull());
  ASSERT_OK(builder_->AppendNull());

  Done();

  ASSERT_OK(ValidateArray(*result_));
  ASSERT_TRUE(result_->IsNull(0));
  ASSERT_TRUE(result_->IsNull(1));

  ASSERT_EQ(0, result_->raw_value_offsets()[0]);
  ASSERT_EQ(0, result_->value_offset(1));
  ASSERT_EQ(0, result_->value_offset(2));

  auto values = result_->values();
  ASSERT_EQ(0, values->length());
  // Values buffer should be non-null
  ASSERT_NE(nullptr, values->data()->buffers[1]);
}

void ValidateBasicListArray(const ListArray* result, const vector<int32_t>& values,
                            const vector<uint8_t>& is_valid) {
  ASSERT_OK(ValidateArray(*result));
  ASSERT_EQ(1, result->null_count());
  ASSERT_EQ(0, result->values()->null_count());

  ASSERT_EQ(3, result->length());
  vector<int32_t> ex_offsets = {0, 3, 3, 7};
  for (size_t i = 0; i < ex_offsets.size(); ++i) {
    ASSERT_EQ(ex_offsets[i], result->value_offset(i));
  }

  for (int i = 0; i < result->length(); ++i) {
    ASSERT_EQ(is_valid[i] == 0, result->IsNull(i));
  }

  ASSERT_EQ(7, result->values()->length());
  auto varr = std::dynamic_pointer_cast<Int32Array>(result->values());

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(values[i], varr->Value(i));
  }
}

TEST_F(TestListArray, TestBasics) {
  vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};
  vector<int> lengths = {3, 0, 4};
  vector<uint8_t> is_valid = {1, 0, 1};

  Int32Builder* vb = checked_cast<Int32Builder*>(builder_->value_builder());

  ASSERT_OK(builder_->Reserve(lengths.size()));
  ASSERT_OK(vb->Reserve(values.size()));

  int pos = 0;
  for (size_t i = 0; i < lengths.size(); ++i) {
    ASSERT_OK(builder_->Append(is_valid[i] > 0));
    for (int j = 0; j < lengths[i]; ++j) {
      ASSERT_OK(vb->Append(values[pos++]));
    }
  }

  Done();
  ValidateBasicListArray(result_.get(), values, is_valid);
}

TEST_F(TestListArray, BulkAppend) {
  vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};
  vector<int> lengths = {3, 0, 4};
  vector<uint8_t> is_valid = {1, 0, 1};
  vector<int32_t> offsets = {0, 3, 3};

  Int32Builder* vb = checked_cast<Int32Builder*>(builder_->value_builder());
  ASSERT_OK(vb->Reserve(values.size()));

  ASSERT_OK(builder_->AppendValues(offsets.data(), offsets.size(), is_valid.data()));
  for (int32_t value : values) {
    ASSERT_OK(vb->Append(value));
  }
  Done();
  ValidateBasicListArray(result_.get(), values, is_valid);
}

TEST_F(TestListArray, BulkAppendInvalid) {
  vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};
  vector<int> lengths = {3, 0, 4};
  vector<uint8_t> is_null = {0, 1, 0};
  vector<uint8_t> is_valid = {1, 0, 1};
  vector<int32_t> offsets = {0, 2, 4};  // should be 0, 3, 3 given the is_null array

  Int32Builder* vb = checked_cast<Int32Builder*>(builder_->value_builder());
  ASSERT_OK(vb->Reserve(values.size()));

  ASSERT_OK(builder_->AppendValues(offsets.data(), offsets.size(), is_valid.data()));
  ASSERT_OK(builder_->AppendValues(offsets.data(), offsets.size(), is_valid.data()));
  for (int32_t value : values) {
    ASSERT_OK(vb->Append(value));
  }

  Done();
  ASSERT_RAISES(Invalid, ValidateArray(*result_));
}

TEST_F(TestListArray, TestZeroLength) {
  // All buffers are null
  Done();
  ASSERT_OK(ValidateArray(*result_));
}

TEST_F(TestListArray, TestBuilderPreserveFieleName) {
  auto list_type_with_name = list(field("counts", int32()));

  std::unique_ptr<ArrayBuilder> tmp;
  ASSERT_OK(MakeBuilder(pool_, list_type_with_name, &tmp));
  builder_.reset(checked_cast<ListBuilder*>(tmp.release()));

  vector<int32_t> values = {1, 2, 4, 8};
  ASSERT_OK(builder_->AppendValues(values.data(), values.size()));

  std::shared_ptr<Array> list_array;
  ASSERT_OK(builder_->Finish(&list_array));

  const auto& type = checked_cast<ListType&>(*list_array->type());
  ASSERT_EQ("counts", type.value_field()->name());
}

}  // namespace arrow
