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
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using std::string;
using std::vector;

using internal::checked_cast;

// ----------------------------------------------------------------------
// String / Binary tests

class TestStringArray : public ::testing::Test {
 public:
  void SetUp() {
    chars_ = {'a', 'b', 'b', 'c', 'c', 'c'};
    offsets_ = {0, 1, 1, 1, 3, 6};
    valid_bytes_ = {1, 1, 0, 1, 1};
    expected_ = {"a", "", "", "bb", "ccc"};

    MakeArray();
  }

  void MakeArray() {
    length_ = static_cast<int64_t>(offsets_.size()) - 1;
    value_buf_ = Buffer::Wrap(chars_);
    offsets_buf_ = Buffer::Wrap(offsets_);
    ASSERT_OK(BitUtil::BytesToBits(valid_bytes_, default_memory_pool(), &null_bitmap_));
    null_count_ = CountNulls(valid_bytes_);

    strings_ = std::make_shared<StringArray>(length_, offsets_buf_, value_buf_,
                                             null_bitmap_, null_count_);
  }

 protected:
  vector<int32_t> offsets_;
  vector<char> chars_;
  vector<uint8_t> valid_bytes_;

  vector<string> expected_;

  std::shared_ptr<Buffer> value_buf_;
  std::shared_ptr<Buffer> offsets_buf_;
  std::shared_ptr<Buffer> null_bitmap_;

  int64_t null_count_;
  int64_t length_;

  std::shared_ptr<StringArray> strings_;
};

TEST_F(TestStringArray, TestArrayBasics) {
  ASSERT_EQ(length_, strings_->length());
  ASSERT_EQ(1, strings_->null_count());
  ASSERT_OK(ValidateArray(*strings_));
}

TEST_F(TestStringArray, TestType) {
  std::shared_ptr<DataType> type = strings_->type();

  ASSERT_EQ(Type::STRING, type->id());
  ASSERT_EQ(Type::STRING, strings_->type_id());
}

TEST_F(TestStringArray, TestListFunctions) {
  int pos = 0;
  for (size_t i = 0; i < expected_.size(); ++i) {
    ASSERT_EQ(pos, strings_->value_offset(i));
    ASSERT_EQ(static_cast<int>(expected_[i].size()), strings_->value_length(i));
    pos += static_cast<int>(expected_[i].size());
  }
}

TEST_F(TestStringArray, TestDestructor) {
  auto arr = std::make_shared<StringArray>(length_, offsets_buf_, value_buf_,
                                           null_bitmap_, null_count_);
}

TEST_F(TestStringArray, TestGetString) {
  for (size_t i = 0; i < expected_.size(); ++i) {
    if (valid_bytes_[i] == 0) {
      ASSERT_TRUE(strings_->IsNull(i));
    } else {
      ASSERT_EQ(expected_[i], strings_->GetString(i));
    }
  }
}

TEST_F(TestStringArray, TestEmptyStringComparison) {
  offsets_ = {0, 0, 0, 0, 0, 0};
  offsets_buf_ = Buffer::Wrap(offsets_);
  length_ = static_cast<int64_t>(offsets_.size() - 1);

  auto strings_a = std::make_shared<StringArray>(length_, offsets_buf_, nullptr,
                                                 null_bitmap_, null_count_);
  auto strings_b = std::make_shared<StringArray>(length_, offsets_buf_, nullptr,
                                                 null_bitmap_, null_count_);
  ASSERT_TRUE(strings_a->Equals(strings_b));
}

TEST_F(TestStringArray, CompareNullByteSlots) {
  StringBuilder builder;
  StringBuilder builder2;
  StringBuilder builder3;

  ASSERT_OK(builder.Append("foo"));
  ASSERT_OK(builder2.Append("foo"));
  ASSERT_OK(builder3.Append("foo"));

  ASSERT_OK(builder.Append("bar"));
  ASSERT_OK(builder2.AppendNull());

  // same length, but different
  ASSERT_OK(builder3.Append("xyz"));

  ASSERT_OK(builder.Append("baz"));
  ASSERT_OK(builder2.Append("baz"));
  ASSERT_OK(builder3.Append("baz"));

  std::shared_ptr<Array> array, array2, array3;
  FinishAndCheckPadding(&builder, &array);
  ASSERT_OK(builder2.Finish(&array2));
  ASSERT_OK(builder3.Finish(&array3));

  const auto& a1 = checked_cast<const StringArray&>(*array);
  const auto& a2 = checked_cast<const StringArray&>(*array2);
  const auto& a3 = checked_cast<const StringArray&>(*array3);

  // The validity bitmaps are the same, the data is different, but the unequal
  // portion is masked out
  StringArray equal_array(3, a1.value_offsets(), a1.value_data(), a2.null_bitmap(), 1);
  StringArray equal_array2(3, a3.value_offsets(), a3.value_data(), a2.null_bitmap(), 1);

  ASSERT_TRUE(equal_array.Equals(equal_array2));
  ASSERT_TRUE(a2.RangeEquals(equal_array2, 0, 3, 0));

  ASSERT_TRUE(equal_array.Array::Slice(1)->Equals(equal_array2.Array::Slice(1)));
  ASSERT_TRUE(
      equal_array.Array::Slice(1)->RangeEquals(0, 2, 0, equal_array2.Array::Slice(1)));
}

TEST_F(TestStringArray, TestSliceGetString) {
  StringBuilder builder;

  ASSERT_OK(builder.Append("a"));
  ASSERT_OK(builder.Append("b"));
  ASSERT_OK(builder.Append("c"));

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));
  auto s = array->Slice(1, 10);
  auto arr = std::dynamic_pointer_cast<StringArray>(s);
  ASSERT_EQ(arr->GetString(0), "b");
}

// ----------------------------------------------------------------------
// String builder tests

class TestStringBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();
    builder_.reset(new StringBuilder(pool_));
  }

  void Done() {
    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder_.get(), &out);

    result_ = std::dynamic_pointer_cast<StringArray>(out);
    ASSERT_OK(ValidateArray(*result_));
  }

 protected:
  std::unique_ptr<StringBuilder> builder_;
  std::shared_ptr<StringArray> result_;
};

TEST_F(TestStringBuilder, TestScalarAppend) {
  vector<string> strings = {"", "bb", "a", "", "ccc"};
  vector<uint8_t> is_null = {0, 0, 0, 1, 0};

  int N = static_cast<int>(strings.size());
  int reps = 1000;

  for (int j = 0; j < reps; ++j) {
    for (int i = 0; i < N; ++i) {
      if (is_null[i]) {
        ASSERT_OK(builder_->AppendNull());
      } else {
        ASSERT_OK(builder_->Append(strings[i]));
      }
    }
  }
  Done();

  ASSERT_EQ(reps * N, result_->length());
  ASSERT_EQ(reps, result_->null_count());
  ASSERT_EQ(reps * 6, result_->value_data()->size());

  int32_t length;
  int32_t pos = 0;
  for (int i = 0; i < N * reps; ++i) {
    if (is_null[i % N]) {
      ASSERT_TRUE(result_->IsNull(i));
    } else {
      ASSERT_FALSE(result_->IsNull(i));
      result_->GetValue(i, &length);
      ASSERT_EQ(pos, result_->value_offset(i));
      ASSERT_EQ(static_cast<int>(strings[i % N].size()), length);
      ASSERT_EQ(strings[i % N], result_->GetString(i));

      pos += length;
    }
  }
}

TEST_F(TestStringBuilder, TestAppendVector) {
  vector<string> strings = {"", "bb", "a", "", "ccc"};
  vector<uint8_t> valid_bytes = {1, 1, 1, 0, 1};

  int N = static_cast<int>(strings.size());
  int reps = 1000;

  for (int j = 0; j < reps; ++j) {
    ASSERT_OK(builder_->AppendValues(strings, valid_bytes.data()));
  }
  Done();

  ASSERT_EQ(reps * N, result_->length());
  ASSERT_EQ(reps, result_->null_count());
  ASSERT_EQ(reps * 6, result_->value_data()->size());

  int32_t length;
  int32_t pos = 0;
  for (int i = 0; i < N * reps; ++i) {
    if (valid_bytes[i % N]) {
      ASSERT_FALSE(result_->IsNull(i));
      result_->GetValue(i, &length);
      ASSERT_EQ(pos, result_->value_offset(i));
      ASSERT_EQ(static_cast<int>(strings[i % N].size()), length);
      ASSERT_EQ(strings[i % N], result_->GetString(i));

      pos += length;
    } else {
      ASSERT_TRUE(result_->IsNull(i));
    }
  }
}

TEST_F(TestStringBuilder, TestAppendCStringsWithValidBytes) {
  const char* strings[] = {nullptr, "aaa", nullptr, "ignored", ""};
  vector<uint8_t> valid_bytes = {1, 1, 1, 0, 1};

  int N = static_cast<int>(sizeof(strings) / sizeof(strings[0]));
  int reps = 1000;

  for (int j = 0; j < reps; ++j) {
    ASSERT_OK(builder_->AppendValues(strings, N, valid_bytes.data()));
  }
  Done();

  ASSERT_EQ(reps * N, result_->length());
  ASSERT_EQ(reps * 3, result_->null_count());
  ASSERT_EQ(reps * 3, result_->value_data()->size());

  int32_t length;
  int32_t pos = 0;
  for (int i = 0; i < N * reps; ++i) {
    auto string = strings[i % N];
    if (string && valid_bytes[i % N]) {
      ASSERT_FALSE(result_->IsNull(i));
      result_->GetValue(i, &length);
      ASSERT_EQ(pos, result_->value_offset(i));
      ASSERT_EQ(static_cast<int32_t>(strlen(string)), length);
      ASSERT_EQ(strings[i % N], result_->GetString(i));

      pos += length;
    } else {
      ASSERT_TRUE(result_->IsNull(i));
    }
  }
}

TEST_F(TestStringBuilder, TestAppendCStringsWithoutValidBytes) {
  const char* strings[] = {"", "bb", "a", nullptr, "ccc"};

  int N = static_cast<int>(sizeof(strings) / sizeof(strings[0]));
  int reps = 1000;

  for (int j = 0; j < reps; ++j) {
    ASSERT_OK(builder_->AppendValues(strings, N));
  }
  Done();

  ASSERT_EQ(reps * N, result_->length());
  ASSERT_EQ(reps, result_->null_count());
  ASSERT_EQ(reps * 6, result_->value_data()->size());

  int32_t length;
  int32_t pos = 0;
  for (int i = 0; i < N * reps; ++i) {
    if (strings[i % N]) {
      ASSERT_FALSE(result_->IsNull(i));
      result_->GetValue(i, &length);
      ASSERT_EQ(pos, result_->value_offset(i));
      ASSERT_EQ(static_cast<int32_t>(strlen(strings[i % N])), length);
      ASSERT_EQ(strings[i % N], result_->GetString(i));

      pos += length;
    } else {
      ASSERT_TRUE(result_->IsNull(i));
    }
  }
}

TEST_F(TestStringBuilder, TestZeroLength) {
  // All buffers are null
  Done();
}

// Binary container type
// TODO(emkornfield) there should be some way to refactor these to avoid code duplicating
// with String
class TestBinaryArray : public ::testing::Test {
 public:
  void SetUp() {
    chars_ = {'a', 'b', 'b', 'c', 'c', 'c'};
    offsets_ = {0, 1, 1, 1, 3, 6};
    valid_bytes_ = {1, 1, 0, 1, 1};
    expected_ = {"a", "", "", "bb", "ccc"};

    MakeArray();
  }

  void MakeArray() {
    length_ = static_cast<int64_t>(offsets_.size() - 1);
    value_buf_ = Buffer::Wrap(chars_);
    offsets_buf_ = Buffer::Wrap(offsets_);

    ASSERT_OK(BitUtil::BytesToBits(valid_bytes_, default_memory_pool(), &null_bitmap_));
    null_count_ = CountNulls(valid_bytes_);

    strings_ = std::make_shared<BinaryArray>(length_, offsets_buf_, value_buf_,
                                             null_bitmap_, null_count_);
  }

 protected:
  vector<int32_t> offsets_;
  vector<char> chars_;
  vector<uint8_t> valid_bytes_;

  vector<string> expected_;

  std::shared_ptr<Buffer> value_buf_;
  std::shared_ptr<Buffer> offsets_buf_;
  std::shared_ptr<Buffer> null_bitmap_;

  int64_t null_count_;
  int64_t length_;

  std::shared_ptr<BinaryArray> strings_;
};

TEST_F(TestBinaryArray, TestArrayBasics) {
  ASSERT_EQ(length_, strings_->length());
  ASSERT_EQ(1, strings_->null_count());
  ASSERT_OK(ValidateArray(*strings_));
}

TEST_F(TestBinaryArray, TestType) {
  std::shared_ptr<DataType> type = strings_->type();

  ASSERT_EQ(Type::BINARY, type->id());
  ASSERT_EQ(Type::BINARY, strings_->type_id());
}

TEST_F(TestBinaryArray, TestListFunctions) {
  size_t pos = 0;
  for (size_t i = 0; i < expected_.size(); ++i) {
    ASSERT_EQ(pos, strings_->value_offset(i));
    ASSERT_EQ(static_cast<int>(expected_[i].size()), strings_->value_length(i));
    pos += expected_[i].size();
  }
}

TEST_F(TestBinaryArray, TestDestructor) {
  auto arr = std::make_shared<BinaryArray>(length_, offsets_buf_, value_buf_,
                                           null_bitmap_, null_count_);
}

TEST_F(TestBinaryArray, TestGetValue) {
  for (size_t i = 0; i < expected_.size(); ++i) {
    if (valid_bytes_[i] == 0) {
      ASSERT_TRUE(strings_->IsNull(i));
    } else {
      ASSERT_FALSE(strings_->IsNull(i));
      ASSERT_EQ(strings_->GetString(i), expected_[i]);
    }
  }
}

TEST_F(TestBinaryArray, TestNullValuesInitialized) {
  for (size_t i = 0; i < expected_.size(); ++i) {
    if (valid_bytes_[i] == 0) {
      ASSERT_TRUE(strings_->IsNull(i));
    } else {
      ASSERT_FALSE(strings_->IsNull(i));
      ASSERT_EQ(strings_->GetString(i), expected_[i]);
    }
  }
  TestInitialized(*strings_);
}

TEST_F(TestBinaryArray, TestPaddingZeroed) { AssertZeroPadded(*strings_); }

TEST_F(TestBinaryArray, TestGetString) {
  for (size_t i = 0; i < expected_.size(); ++i) {
    if (valid_bytes_[i] == 0) {
      ASSERT_TRUE(strings_->IsNull(i));
    } else {
      std::string val = strings_->GetString(i);
      ASSERT_EQ(0, std::memcmp(expected_[i].data(), val.c_str(), val.size()));
    }
  }
}

TEST_F(TestBinaryArray, TestEqualsEmptyStrings) {
  BinaryBuilder builder;

  string empty_string("");
  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(builder.Append(empty_string));
  }

  std::shared_ptr<Array> left_arr;
  FinishAndCheckPadding(&builder, &left_arr);

  const BinaryArray& left = checked_cast<const BinaryArray&>(*left_arr);
  std::shared_ptr<Array> right =
      std::make_shared<BinaryArray>(left.length(), left.value_offsets(), nullptr,
                                    left.null_bitmap(), left.null_count());

  ASSERT_TRUE(left.Equals(right));
  ASSERT_TRUE(left.RangeEquals(0, left.length(), 0, right));
}

class TestBinaryBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();
    builder_.reset(new BinaryBuilder(pool_));
  }

  void Done() {
    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder_.get(), &out);

    result_ = std::dynamic_pointer_cast<BinaryArray>(out);
    ASSERT_OK(ValidateArray(*result_));
  }

 protected:
  std::unique_ptr<BinaryBuilder> builder_;
  std::shared_ptr<BinaryArray> result_;
};

TEST_F(TestBinaryBuilder, TestScalarAppend) {
  vector<string> strings = {"", "bb", "a", "", "ccc"};
  vector<uint8_t> is_null = {0, 0, 0, 1, 0};

  int N = static_cast<int>(strings.size());
  int reps = 10;

  for (int j = 0; j < reps; ++j) {
    for (int i = 0; i < N; ++i) {
      if (is_null[i]) {
        ASSERT_OK(builder_->AppendNull());
      } else {
        ASSERT_OK(builder_->Append(strings[i]));
      }
    }
  }
  Done();
  ASSERT_OK(ValidateArray(*result_));
  ASSERT_EQ(reps * N, result_->length());
  ASSERT_EQ(reps, result_->null_count());
  ASSERT_EQ(reps * 6, result_->value_data()->size());

  int32_t length;
  for (int i = 0; i < N * reps; ++i) {
    if (is_null[i % N]) {
      ASSERT_TRUE(result_->IsNull(i));
    } else {
      ASSERT_FALSE(result_->IsNull(i));
      const uint8_t* vals = result_->GetValue(i, &length);
      ASSERT_EQ(static_cast<int>(strings[i % N].size()), length);
      ASSERT_EQ(0, std::memcmp(vals, strings[i % N].data(), length));
    }
  }
}

TEST_F(TestBinaryBuilder, TestScalarAppendUnsafe) {
  vector<string> strings = {"", "bb", "a", "", "ccc"};
  vector<uint8_t> is_null = {0, 0, 0, 1, 0};

  int N = static_cast<int>(strings.size());
  int reps = 13;
  int total_length = 0;
  for (auto&& s : strings) total_length += static_cast<int>(s.size());

  ASSERT_OK(builder_->Reserve(N * reps));
  ASSERT_OK(builder_->ReserveData(total_length * reps));

  for (int j = 0; j < reps; ++j) {
    for (int i = 0; i < N; ++i) {
      if (is_null[i]) {
        builder_->UnsafeAppendNull();
      } else {
        builder_->UnsafeAppend(strings[i]);
      }
    }
  }
  ASSERT_EQ(builder_->value_data_length(), total_length * reps);
  Done();
  ASSERT_OK(ValidateArray(*result_));
  ASSERT_EQ(reps * N, result_->length());
  ASSERT_EQ(reps, result_->null_count());
  ASSERT_EQ(reps * total_length, result_->value_data()->size());

  int32_t length;
  for (int i = 0; i < N * reps; ++i) {
    if (is_null[i % N]) {
      ASSERT_TRUE(result_->IsNull(i));
    } else {
      ASSERT_FALSE(result_->IsNull(i));
      const uint8_t* vals = result_->GetValue(i, &length);
      ASSERT_EQ(static_cast<int>(strings[i % N].size()), length);
      ASSERT_EQ(0, std::memcmp(vals, strings[i % N].data(), length));
    }
  }
}

TEST_F(TestBinaryBuilder, TestCapacityReserve) {
  vector<string> strings = {"aaaaa", "bbbbbbbbbb", "ccccccccccccccc", "dddddddddd"};
  int N = static_cast<int>(strings.size());
  int reps = 15;
  int64_t length = 0;
  int64_t capacity = 1000;
  int64_t expected_capacity = BitUtil::RoundUpToMultipleOf64(capacity);

  ASSERT_OK(builder_->ReserveData(capacity));

  ASSERT_EQ(length, builder_->value_data_length());
  ASSERT_EQ(expected_capacity, builder_->value_data_capacity());

  for (int j = 0; j < reps; ++j) {
    for (int i = 0; i < N; ++i) {
      ASSERT_OK(builder_->Append(strings[i]));
      length += static_cast<int>(strings[i].size());

      ASSERT_EQ(length, builder_->value_data_length());
      ASSERT_EQ(expected_capacity, builder_->value_data_capacity());
    }
  }

  int extra_capacity = 500;
  expected_capacity = BitUtil::RoundUpToMultipleOf64(length + extra_capacity);

  ASSERT_OK(builder_->ReserveData(extra_capacity));

  ASSERT_EQ(length, builder_->value_data_length());
  ASSERT_EQ(expected_capacity, builder_->value_data_capacity());

  Done();

  ASSERT_EQ(reps * N, result_->length());
  ASSERT_EQ(0, result_->null_count());
  ASSERT_EQ(reps * 40, result_->value_data()->size());

  // Capacity is shrunk after `Finish`
  ASSERT_EQ(640, result_->value_data()->capacity());
}

TEST_F(TestBinaryBuilder, TestZeroLength) {
  // All buffers are null
  Done();
}

// ----------------------------------------------------------------------
// Slice tests

template <typename TYPE>
void CheckSliceEquality() {
  using Traits = TypeTraits<TYPE>;
  using BuilderType = typename Traits::BuilderType;

  BuilderType builder;

  vector<string> strings = {"foo", "", "bar", "baz", "qux", ""};
  vector<uint8_t> is_null = {0, 1, 0, 1, 0, 0};

  int N = static_cast<int>(strings.size());
  int reps = 10;

  for (int j = 0; j < reps; ++j) {
    for (int i = 0; i < N; ++i) {
      if (is_null[i]) {
        ASSERT_OK(builder.AppendNull());
      } else {
        ASSERT_OK(builder.Append(strings[i]));
      }
    }
  }

  std::shared_ptr<Array> array;
  FinishAndCheckPadding(&builder, &array);

  std::shared_ptr<Array> slice, slice2;

  slice = array->Slice(5);
  slice2 = array->Slice(5);
  ASSERT_EQ(N * reps - 5, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(5, slice->length(), 0, slice));

  // Chained slices
  slice2 = array->Slice(2)->Slice(3);
  ASSERT_TRUE(slice->Equals(slice2));

  slice = array->Slice(5, 20);
  slice2 = array->Slice(5, 20);
  ASSERT_EQ(20, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(5, 25, 0, slice));
}

TEST_F(TestBinaryArray, TestSliceEquality) { CheckSliceEquality<BinaryType>(); }

TEST_F(TestStringArray, TestSliceEquality) { CheckSliceEquality<BinaryType>(); }

TEST_F(TestBinaryArray, LengthZeroCtor) { BinaryArray array(0, nullptr, nullptr); }

// ----------------------------------------------------------------------
// ChunkedBinaryBuilder tests

class TestChunkedBinaryBuilder : public ::testing::Test {
 public:
  void SetUp() {}

  void Init(int32_t chunksize) {
    builder_.reset(new internal::ChunkedBinaryBuilder(chunksize));
  }

 protected:
  std::unique_ptr<internal::ChunkedBinaryBuilder> builder_;
};

TEST_F(TestChunkedBinaryBuilder, BasicOperation) {
  const int32_t chunksize = 1000;
  Init(chunksize);

  const int elem_size = 10;
  uint8_t buf[elem_size];

  BinaryBuilder unchunked_builder;

  const int iterations = 1000;
  for (int i = 0; i < iterations; ++i) {
    random_bytes(elem_size, i, buf);

    ASSERT_OK(unchunked_builder.Append(buf, elem_size));
    ASSERT_OK(builder_->Append(buf, elem_size));
  }

  std::shared_ptr<Array> unchunked;
  ASSERT_OK(unchunked_builder.Finish(&unchunked));

  ArrayVector chunks;
  ASSERT_OK(builder_->Finish(&chunks));

  // This assumes that everything is evenly divisible
  ArrayVector expected_chunks;
  const int elems_per_chunk = chunksize / elem_size;
  for (int i = 0; i < iterations / elems_per_chunk; ++i) {
    expected_chunks.emplace_back(unchunked->Slice(i * elems_per_chunk, elems_per_chunk));
  }

  ASSERT_EQ(expected_chunks.size(), chunks.size());
  for (size_t i = 0; i < chunks.size(); ++i) {
    AssertArraysEqual(*expected_chunks[i], *chunks[i]);
  }
}

TEST_F(TestChunkedBinaryBuilder, NoData) {
  Init(1000);

  ArrayVector chunks;
  ASSERT_OK(builder_->Finish(&chunks));

  ASSERT_EQ(1, chunks.size());
  ASSERT_EQ(0, chunks[0]->length());
}

TEST_F(TestChunkedBinaryBuilder, LargeElements) {
  Init(100);

  const int bufsize = 101;
  uint8_t buf[bufsize];

  const int iterations = 100;
  for (int i = 0; i < iterations; ++i) {
    random_bytes(bufsize, i, buf);
    ASSERT_OK(builder_->Append(buf, bufsize));
  }

  ArrayVector chunks;
  ASSERT_OK(builder_->Finish(&chunks));
  ASSERT_EQ(iterations, static_cast<int>(chunks.size()));

  int64_t total_data_size = 0;
  for (auto chunk : chunks) {
    ASSERT_EQ(1, chunk->length());
    total_data_size +=
        static_cast<int64_t>(static_cast<const BinaryArray&>(*chunk).GetView(0).size());
  }
  ASSERT_EQ(iterations * bufsize, total_data_size);
}

TEST(TestChunkedStringBuilder, BasicOperation) {
  const int chunksize = 100;
  internal::ChunkedStringBuilder builder(chunksize);

  std::string value = "0123456789";

  const int iterations = 100;
  for (int i = 0; i < iterations; ++i) {
    ASSERT_OK(builder.Append(value));
  }

  ArrayVector chunks;
  ASSERT_OK(builder.Finish(&chunks));

  ASSERT_EQ(10, chunks.size());

  // Type is correct
  for (auto chunk : chunks) {
    ASSERT_TRUE(chunk->type()->Equals(*::arrow::utf8()));
  }
}

}  // namespace arrow
