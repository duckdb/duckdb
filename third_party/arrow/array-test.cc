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
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer-builder.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/ipc/test-common.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/lazy.h"

// This file is compiled together with array-*-test.cc into a single
// executable array-test.

namespace arrow {

using std::string;
using std::vector;

using internal::checked_cast;

class TestArray : public ::testing::Test {
 public:
  void SetUp() { pool_ = default_memory_pool(); }

 protected:
  MemoryPool* pool_;
};

TEST_F(TestArray, TestNullCount) {
  // These are placeholders
  auto data = std::make_shared<Buffer>(nullptr, 0);
  auto null_bitmap = std::make_shared<Buffer>(nullptr, 0);

  std::unique_ptr<Int32Array> arr(new Int32Array(100, data, null_bitmap, 10));
  ASSERT_EQ(10, arr->null_count());

  std::unique_ptr<Int32Array> arr_no_nulls(new Int32Array(100, data));
  ASSERT_EQ(0, arr_no_nulls->null_count());
}

TEST_F(TestArray, TestLength) {
  // Placeholder buffer
  auto data = std::make_shared<Buffer>(nullptr, 0);

  std::unique_ptr<Int32Array> arr(new Int32Array(100, data));
  ASSERT_EQ(arr->length(), 100);
}

Status MakeArrayFromValidBytes(const vector<uint8_t>& v, MemoryPool* pool,
                               std::shared_ptr<Array>* out) {
  int64_t null_count = v.size() - std::accumulate(v.begin(), v.end(), 0);

  std::shared_ptr<Buffer> null_buf;
  RETURN_NOT_OK(BitUtil::BytesToBits(v, default_memory_pool(), &null_buf));

  TypedBufferBuilder<int32_t> value_builder(pool);
  for (size_t i = 0; i < v.size(); ++i) {
    RETURN_NOT_OK(value_builder.Append(0));
  }

  std::shared_ptr<Buffer> values;
  RETURN_NOT_OK(value_builder.Finish(&values));
  *out = std::make_shared<Int32Array>(v.size(), values, null_buf, null_count);
  return Status::OK();
}

TEST_F(TestArray, TestEquality) {
  std::shared_ptr<Array> array, equal_array, unequal_array;

  ASSERT_OK(MakeArrayFromValidBytes({1, 0, 1, 1, 0, 1, 0, 0}, pool_, &array));
  ASSERT_OK(MakeArrayFromValidBytes({1, 0, 1, 1, 0, 1, 0, 0}, pool_, &equal_array));
  ASSERT_OK(MakeArrayFromValidBytes({1, 1, 1, 1, 0, 1, 0, 0}, pool_, &unequal_array));

  EXPECT_TRUE(array->Equals(array));
  EXPECT_TRUE(array->Equals(equal_array));
  EXPECT_TRUE(equal_array->Equals(array));
  EXPECT_FALSE(equal_array->Equals(unequal_array));
  EXPECT_FALSE(unequal_array->Equals(equal_array));
  EXPECT_TRUE(array->RangeEquals(4, 8, 4, unequal_array));
  EXPECT_FALSE(array->RangeEquals(0, 4, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(0, 8, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(1, 2, 1, unequal_array));

  auto timestamp_ns_array = std::make_shared<NumericArray<TimestampType>>(
      timestamp(TimeUnit::NANO), array->length(), array->data()->buffers[1],
      array->data()->buffers[0], array->null_count());
  auto timestamp_us_array = std::make_shared<NumericArray<TimestampType>>(
      timestamp(TimeUnit::MICRO), array->length(), array->data()->buffers[1],
      array->data()->buffers[0], array->null_count());
  ASSERT_FALSE(array->Equals(timestamp_ns_array));
  // ARROW-2567: Ensure that not only the type id but also the type equality
  // itself is checked.
  ASSERT_FALSE(timestamp_us_array->Equals(timestamp_ns_array));
}

TEST_F(TestArray, TestNullArrayEquality) {
  auto array_1 = std::make_shared<NullArray>(10);
  auto array_2 = std::make_shared<NullArray>(10);
  auto array_3 = std::make_shared<NullArray>(20);

  EXPECT_TRUE(array_1->Equals(array_1));
  EXPECT_TRUE(array_1->Equals(array_2));
  EXPECT_FALSE(array_1->Equals(array_3));
}

TEST_F(TestArray, SliceRecomputeNullCount) {
  vector<uint8_t> valid_bytes = {1, 0, 1, 1, 0, 1, 0, 0, 0};

  std::shared_ptr<Array> array;
  ASSERT_OK(MakeArrayFromValidBytes(valid_bytes, pool_, &array));

  ASSERT_EQ(5, array->null_count());

  auto slice = array->Slice(1, 4);
  ASSERT_EQ(2, slice->null_count());

  slice = array->Slice(4);
  ASSERT_EQ(4, slice->null_count());

  slice = array->Slice(0);
  ASSERT_EQ(5, slice->null_count());

  // No bitmap, compute 0
  std::shared_ptr<Buffer> data;
  const int kBufferSize = 64;
  ASSERT_OK(AllocateBuffer(pool_, kBufferSize, &data));
  memset(data->mutable_data(), 0, kBufferSize);

  auto arr = std::make_shared<Int32Array>(16, data, nullptr, -1);
  ASSERT_EQ(0, arr->null_count());
}

TEST_F(TestArray, NullArraySliceNullCount) {
  auto null_arr = std::make_shared<NullArray>(10);
  auto null_arr_sliced = null_arr->Slice(3, 6);

  // The internal null count is 6, does not require recomputation
  ASSERT_EQ(6, null_arr_sliced->data()->null_count);

  ASSERT_EQ(6, null_arr_sliced->null_count());
}

TEST_F(TestArray, TestIsNullIsValid) {
  // clang-format off
  vector<uint8_t> null_bitmap = {1, 0, 1, 1, 0, 1, 0, 0,
                                 1, 0, 1, 1, 0, 1, 0, 0,
                                 1, 0, 1, 1, 0, 1, 0, 0,
                                 1, 0, 1, 1, 0, 1, 0, 0,
                                 1, 0, 0, 1};
  // clang-format on
  int64_t null_count = 0;
  for (uint8_t x : null_bitmap) {
    if (x == 0) {
      ++null_count;
    }
  }

  std::shared_ptr<Buffer> null_buf;
  ASSERT_OK(BitUtil::BytesToBits(null_bitmap, default_memory_pool(), &null_buf));

  std::unique_ptr<Array> arr;
  arr.reset(new Int32Array(null_bitmap.size(), nullptr, null_buf, null_count));

  ASSERT_EQ(null_count, arr->null_count());
  ASSERT_EQ(5, null_buf->size());

  ASSERT_TRUE(arr->null_bitmap()->Equals(*null_buf.get()));

  for (size_t i = 0; i < null_bitmap.size(); ++i) {
    EXPECT_EQ(null_bitmap[i] != 0, !arr->IsNull(i)) << i;
    EXPECT_EQ(null_bitmap[i] != 0, arr->IsValid(i)) << i;
  }
}

TEST_F(TestArray, TestIsNullIsValidNoNulls) {
  const int64_t size = 10;

  std::unique_ptr<Array> arr;
  arr.reset(new Int32Array(size, nullptr, nullptr, 0));

  for (size_t i = 0; i < size; ++i) {
    EXPECT_TRUE(arr->IsValid(i));
    EXPECT_FALSE(arr->IsNull(i));
  }
}

TEST_F(TestArray, BuildLargeInMemoryArray) {
#ifdef NDEBUG
  const int64_t length = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1;
#elif !defined(ARROW_VALGRIND)
  // use a smaller size since the insert function isn't optimized properly on debug and
  // the test takes a long time to complete
  const int64_t length = 2 << 24;
#else
  // use an even smaller size with valgrind
  const int64_t length = 2 << 20;
#endif

  BooleanBuilder builder;
  std::vector<bool> zeros(length);
  ASSERT_OK(builder.AppendValues(zeros));

  std::shared_ptr<Array> result;
  FinishAndCheckPadding(&builder, &result);

  ASSERT_EQ(length, result->length());
}

TEST_F(TestArray, TestCopy) {}

// ----------------------------------------------------------------------
// Null type tests

TEST(TestNullBuilder, Basics) {
  NullBuilder builder;
  std::shared_ptr<Array> array;

  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Append(nullptr));
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Finish(&array));

  const auto& null_array = checked_cast<NullArray&>(*array);
  ASSERT_EQ(null_array.length(), 3);
  ASSERT_EQ(null_array.null_count(), 3);
}

// ----------------------------------------------------------------------
// Primitive type tests

TEST_F(TestBuilder, TestReserve) {
  UInt8Builder builder(pool_);

  ASSERT_OK(builder.Resize(1000));
  ASSERT_EQ(1000, builder.capacity());

  // Builder only contains 0 elements, but calling Reserve will result in a round
  // up to next power of 2
  ASSERT_OK(builder.Reserve(1030));
  ASSERT_EQ(BitUtil::NextPower2(1030), builder.capacity());
}

TEST_F(TestBuilder, TestResizeDownsize) {
  UInt8Builder builder(pool_);

  ASSERT_OK(builder.Resize(1000));
  ASSERT_EQ(1000, builder.capacity());

  // Can't downsize.
  ASSERT_RAISES(Invalid, builder.Resize(500));
}

template <typename Attrs>
class TestPrimitiveBuilder : public TestBuilder {
 public:
  typedef typename Attrs::ArrayType ArrayType;
  typedef typename Attrs::BuilderType BuilderType;
  typedef typename Attrs::T T;
  typedef typename Attrs::Type Type;

  virtual void SetUp() {
    TestBuilder::SetUp();

    type_ = Attrs::type();

    std::unique_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_.reset(checked_cast<BuilderType*>(tmp.release()));

    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_nn_.reset(checked_cast<BuilderType*>(tmp.release()));
  }

  void RandomData(int64_t N, double pct_null = 0.1) {
    Attrs::draw(N, &draws_);

    valid_bytes_.resize(static_cast<size_t>(N));
    random_null_bytes(N, pct_null, valid_bytes_.data());
  }

  void Check(const std::unique_ptr<BuilderType>& builder, bool nullable) {
    int64_t size = builder->length();
    auto ex_data = Buffer::Wrap(draws_.data(), size);

    std::shared_ptr<Buffer> ex_null_bitmap;
    int64_t ex_null_count = 0;

    if (nullable) {
      ASSERT_OK(
          BitUtil::BytesToBits(valid_bytes_, default_memory_pool(), &ex_null_bitmap));
      ex_null_count = CountNulls(valid_bytes_);
    } else {
      ex_null_bitmap = nullptr;
    }

    auto expected =
        std::make_shared<ArrayType>(size, ex_data, ex_null_bitmap, ex_null_count);

    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder.get(), &out);

    std::shared_ptr<ArrayType> result = std::dynamic_pointer_cast<ArrayType>(out);

    // Builder is now reset
    ASSERT_EQ(0, builder->length());
    ASSERT_EQ(0, builder->capacity());
    ASSERT_EQ(0, builder->null_count());

    ASSERT_EQ(ex_null_count, result->null_count());
    ASSERT_TRUE(result->Equals(*expected));
  }

  void FlipValue(T* ptr) {
    auto byteptr = reinterpret_cast<uint8_t*>(ptr);
    *byteptr = static_cast<uint8_t>(~*byteptr);
  }

 protected:
  std::unique_ptr<BuilderType> builder_;
  std::unique_ptr<BuilderType> builder_nn_;

  vector<T> draws_;
  vector<uint8_t> valid_bytes_;
};

/// \brief uint8_t isn't a valid template parameter to uniform_int_distribution, so
/// we use SampleType to determine which kind of integer to use to sample.
template <typename T,
          typename = typename std::enable_if<std::is_integral<T>::value, T>::type>
struct UniformIntSampleType {
  using type = T;
};

template <>
struct UniformIntSampleType<uint8_t> {
  using type = uint16_t;
};

template <>
struct UniformIntSampleType<int8_t> {
  using type = int16_t;
};

#define PTYPE_DECL(CapType, c_type)     \
  typedef CapType##Array ArrayType;     \
  typedef CapType##Builder BuilderType; \
  typedef CapType##Type Type;           \
  typedef c_type T;                     \
                                        \
  static std::shared_ptr<DataType> type() { return std::make_shared<Type>(); }

#define PINT_DECL(CapType, c_type)                                                 \
  struct P##CapType {                                                              \
    PTYPE_DECL(CapType, c_type)                                                    \
    static void draw(int64_t N, vector<T>* draws) {                                \
      using sample_type = typename UniformIntSampleType<c_type>::type;             \
      const T lower = std::numeric_limits<T>::min();                               \
      const T upper = std::numeric_limits<T>::max();                               \
      randint(N, static_cast<sample_type>(lower), static_cast<sample_type>(upper), \
              draws);                                                              \
    }                                                                              \
  }

#define PFLOAT_DECL(CapType, c_type, LOWER, UPPER)  \
  struct P##CapType {                               \
    PTYPE_DECL(CapType, c_type)                     \
    static void draw(int64_t N, vector<T>* draws) { \
      random_real(N, 0, LOWER, UPPER, draws);       \
    }                                               \
  }

PINT_DECL(UInt8, uint8_t);
PINT_DECL(UInt16, uint16_t);
PINT_DECL(UInt32, uint32_t);
PINT_DECL(UInt64, uint64_t);

PINT_DECL(Int8, int8_t);
PINT_DECL(Int16, int16_t);
PINT_DECL(Int32, int32_t);
PINT_DECL(Int64, int64_t);

PFLOAT_DECL(Float, float, -1000.0f, 1000.0f);
PFLOAT_DECL(Double, double, -1000.0, 1000.0);

struct PBoolean {
  PTYPE_DECL(Boolean, uint8_t)
};

template <>
void TestPrimitiveBuilder<PBoolean>::RandomData(int64_t N, double pct_null) {
  draws_.resize(static_cast<size_t>(N));
  valid_bytes_.resize(static_cast<size_t>(N));

  random_null_bytes(N, 0.5, draws_.data());
  random_null_bytes(N, pct_null, valid_bytes_.data());
}

template <>
void TestPrimitiveBuilder<PBoolean>::FlipValue(T* ptr) {
  *ptr = !*ptr;
}

template <>
void TestPrimitiveBuilder<PBoolean>::Check(const std::unique_ptr<BooleanBuilder>& builder,
                                           bool nullable) {
  const int64_t size = builder->length();

  // Build expected result array
  std::shared_ptr<Buffer> ex_data;
  std::shared_ptr<Buffer> ex_null_bitmap;
  int64_t ex_null_count = 0;

  ASSERT_OK(BitUtil::BytesToBits(draws_, default_memory_pool(), &ex_data));
  if (nullable) {
    ASSERT_OK(BitUtil::BytesToBits(valid_bytes_, default_memory_pool(), &ex_null_bitmap));
    ex_null_count = CountNulls(valid_bytes_);
  } else {
    ex_null_bitmap = nullptr;
  }
  auto expected =
      std::make_shared<BooleanArray>(size, ex_data, ex_null_bitmap, ex_null_count);
  ASSERT_EQ(size, expected->length());

  // Finish builder and check result array
  std::shared_ptr<Array> out;
  FinishAndCheckPadding(builder.get(), &out);

  std::shared_ptr<BooleanArray> result = std::dynamic_pointer_cast<BooleanArray>(out);

  ASSERT_EQ(ex_null_count, result->null_count());
  ASSERT_EQ(size, result->length());

  for (int64_t i = 0; i < size; ++i) {
    if (nullable) {
      ASSERT_EQ(valid_bytes_[i] == 0, result->IsNull(i)) << i;
    } else {
      ASSERT_FALSE(result->IsNull(i));
    }
    if (!result->IsNull(i)) {
      bool actual = BitUtil::GetBit(result->values()->data(), i);
      ASSERT_EQ(draws_[i] != 0, actual) << i;
    }
  }
  AssertArraysEqual(*result, *expected);

  // buffers are correctly sized
  ASSERT_EQ(result->data()->buffers[0]->size(), BitUtil::BytesForBits(size));
  ASSERT_EQ(result->data()->buffers[1]->size(), BitUtil::BytesForBits(size));

  // Builder is now reset
  ASSERT_EQ(0, builder->length());
  ASSERT_EQ(0, builder->capacity());
  ASSERT_EQ(0, builder->null_count());
}

typedef ::testing::Types<PBoolean, PUInt8, PUInt16, PUInt32, PUInt64, PInt8, PInt16,
                         PInt32, PInt64, PFloat, PDouble>
    Primitives;

TYPED_TEST_CASE(TestPrimitiveBuilder, Primitives);

TYPED_TEST(TestPrimitiveBuilder, TestInit) {
  int64_t n = 1000;
  ASSERT_OK(this->builder_->Reserve(n));
  ASSERT_EQ(BitUtil::NextPower2(n), this->builder_->capacity());

  // unsure if this should go in all builder classes
  ASSERT_EQ(0, this->builder_->num_children());
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendNull) {
  int64_t size = 1000;
  for (int64_t i = 0; i < size; ++i) {
    ASSERT_OK(this->builder_->AppendNull());
  }

  std::shared_ptr<Array> out;
  FinishAndCheckPadding(this->builder_.get(), &out);
  auto result = std::dynamic_pointer_cast<typename TypeParam::ArrayType>(out);

  for (int64_t i = 0; i < size; ++i) {
    ASSERT_TRUE(result->IsNull(i)) << i;
  }
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendNulls) {
  const int64_t size = 10;
  ASSERT_OK(this->builder_->AppendNulls(size));

  std::shared_ptr<Array> result;
  FinishAndCheckPadding(this->builder_.get(), &result);

  for (int64_t i = 0; i < size; ++i) {
    ASSERT_FALSE(result->IsValid(i));
  }
}

TYPED_TEST(TestPrimitiveBuilder, TestArrayDtorDealloc) {
  DECL_T();

  int64_t size = 1000;

  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;

  int64_t memory_before = this->pool_->bytes_allocated();

  this->RandomData(size);
  ASSERT_OK(this->builder_->Reserve(size));

  int64_t i;
  for (i = 0; i < size; ++i) {
    if (valid_bytes[i] > 0) {
      ASSERT_OK(this->builder_->Append(draws[i]));
    } else {
      ASSERT_OK(this->builder_->AppendNull());
    }
  }

  do {
    std::shared_ptr<Array> result;
    FinishAndCheckPadding(this->builder_.get(), &result);
  } while (false);

  ASSERT_EQ(memory_before, this->pool_->bytes_allocated());
}

TYPED_TEST(TestPrimitiveBuilder, Equality) {
  DECL_T();

  const int64_t size = 1000;
  this->RandomData(size);
  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;
  std::shared_ptr<Array> array, equal_array, unequal_array;
  auto builder = this->builder_.get();
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &array));
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &equal_array));

  // Make the not equal array by negating the first valid element with itself.
  const auto first_valid = std::find_if(valid_bytes.begin(), valid_bytes.end(),
                                        [](uint8_t valid) { return valid > 0; });
  const int64_t first_valid_idx = std::distance(valid_bytes.begin(), first_valid);
  // This should be true with a very high probability, but might introduce flakiness
  ASSERT_LT(first_valid_idx, size - 1);
  this->FlipValue(&draws[first_valid_idx]);
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &unequal_array));

  // test normal equality
  EXPECT_TRUE(array->Equals(array));
  EXPECT_TRUE(array->Equals(equal_array));
  EXPECT_TRUE(equal_array->Equals(array));
  EXPECT_FALSE(equal_array->Equals(unequal_array));
  EXPECT_FALSE(unequal_array->Equals(equal_array));

  // Test range equality
  EXPECT_FALSE(array->RangeEquals(0, first_valid_idx + 1, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(first_valid_idx, size, first_valid_idx, unequal_array));
  EXPECT_TRUE(array->RangeEquals(0, first_valid_idx, 0, unequal_array));
  EXPECT_TRUE(
      array->RangeEquals(first_valid_idx + 1, size, first_valid_idx + 1, unequal_array));
}

TYPED_TEST(TestPrimitiveBuilder, SliceEquality) {
  DECL_T();

  const int64_t size = 1000;
  this->RandomData(size);
  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;
  auto builder = this->builder_.get();

  std::shared_ptr<Array> array;
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &array));

  std::shared_ptr<Array> slice, slice2;

  slice = array->Slice(5);
  slice2 = array->Slice(5);
  ASSERT_EQ(size - 5, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(5, array->length(), 0, slice));

  // Chained slices
  slice2 = array->Slice(2)->Slice(3);
  ASSERT_TRUE(slice->Equals(slice2));

  slice = array->Slice(5, 10);
  slice2 = array->Slice(5, 10);
  ASSERT_EQ(10, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(5, 15, 0, slice));
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendScalar) {
  DECL_T();

  const int64_t size = 10000;

  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;

  this->RandomData(size);

  ASSERT_OK(this->builder_->Reserve(1000));
  ASSERT_OK(this->builder_nn_->Reserve(1000));

  int64_t null_count = 0;
  // Append the first 1000
  for (size_t i = 0; i < 1000; ++i) {
    if (valid_bytes[i] > 0) {
      ASSERT_OK(this->builder_->Append(draws[i]));
    } else {
      ASSERT_OK(this->builder_->AppendNull());
      ++null_count;
    }
    ASSERT_OK(this->builder_nn_->Append(draws[i]));
  }

  ASSERT_EQ(null_count, this->builder_->null_count());

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1024, this->builder_->capacity());

  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1024, this->builder_nn_->capacity());

  ASSERT_OK(this->builder_->Reserve(size - 1000));
  ASSERT_OK(this->builder_nn_->Reserve(size - 1000));

  // Append the next 9000
  for (size_t i = 1000; i < size; ++i) {
    if (valid_bytes[i] > 0) {
      ASSERT_OK(this->builder_->Append(draws[i]));
    } else {
      ASSERT_OK(this->builder_->AppendNull());
    }
    ASSERT_OK(this->builder_nn_->Append(draws[i]));
  }

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_->capacity());

  ASSERT_EQ(size, this->builder_nn_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_nn_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValues) {
  DECL_T();

  int64_t size = 10000;
  this->RandomData(size);

  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;

  // first slug
  int64_t K = 1000;

  ASSERT_OK(this->builder_->AppendValues(draws.data(), K, valid_bytes.data()));
  ASSERT_OK(this->builder_nn_->AppendValues(draws.data(), K));

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1024, this->builder_->capacity());

  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1024, this->builder_nn_->capacity());

  // Append the next 9000
  ASSERT_OK(
      this->builder_->AppendValues(draws.data() + K, size - K, valid_bytes.data() + K));
  ASSERT_OK(this->builder_nn_->AppendValues(draws.data() + K, size - K));

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_->capacity());

  ASSERT_EQ(size, this->builder_nn_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_nn_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValuesIter) {
  int64_t size = 10000;
  this->RandomData(size);

  ASSERT_OK(this->builder_->AppendValues(this->draws_.begin(), this->draws_.end(),
                                         this->valid_bytes_.begin()));
  ASSERT_OK(this->builder_nn_->AppendValues(this->draws_.begin(), this->draws_.end()));

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValuesIterNullValid) {
  int64_t size = 10000;
  this->RandomData(size);

  ASSERT_OK(this->builder_nn_->AppendValues(this->draws_.begin(),
                                            this->draws_.begin() + size / 2,
                                            static_cast<uint8_t*>(nullptr)));

  ASSERT_EQ(BitUtil::NextPower2(size / 2), this->builder_nn_->capacity());

  ASSERT_OK(this->builder_nn_->AppendValues(this->draws_.begin() + size / 2,
                                            this->draws_.end(),
                                            static_cast<uint64_t*>(nullptr)));

  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValuesLazyIter) {
  DECL_T();

  int64_t size = 10000;
  this->RandomData(size);

  auto& draws = this->draws_;
  auto& valid_bytes = this->valid_bytes_;

  auto halve = [&draws](int64_t index) { return draws[index] / 2; };
  auto lazy_iter = internal::MakeLazyRange(halve, size);

  ASSERT_OK(this->builder_->AppendValues(lazy_iter.begin(), lazy_iter.end(),
                                         valid_bytes.begin()));

  std::vector<T> halved;
  transform(draws.begin(), draws.end(), back_inserter(halved),
            [](T in) { return in / 2; });

  std::shared_ptr<Array> result;
  FinishAndCheckPadding(this->builder_.get(), &result);

  std::shared_ptr<Array> expected;
  ASSERT_OK(
      this->builder_->AppendValues(halved.data(), halved.size(), valid_bytes.data()));
  FinishAndCheckPadding(this->builder_.get(), &expected);

  ASSERT_TRUE(expected->Equals(result));
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValuesIterConverted) {
  DECL_T();
  // find type we can safely convert the tested values to and from
  using conversion_type =
      typename std::conditional<std::is_floating_point<T>::value, double,
                                typename std::conditional<std::is_unsigned<T>::value,
                                                          uint64_t, int64_t>::type>::type;

  int64_t size = 10000;
  this->RandomData(size);

  // append convertible values
  vector<conversion_type> draws_converted(this->draws_.begin(), this->draws_.end());
  vector<int32_t> valid_bytes_converted(this->valid_bytes_.begin(),
                                        this->valid_bytes_.end());

  auto cast_values = internal::MakeLazyRange(
      [&draws_converted](int64_t index) {
        return static_cast<T>(draws_converted[index]);
      },
      size);
  auto cast_valid = internal::MakeLazyRange(
      [&valid_bytes_converted](int64_t index) {
        return static_cast<bool>(valid_bytes_converted[index]);
      },
      size);

  ASSERT_OK(this->builder_->AppendValues(cast_values.begin(), cast_values.end(),
                                         cast_valid.begin()));
  ASSERT_OK(this->builder_nn_->AppendValues(cast_values.begin(), cast_values.end()));

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_->capacity());

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestZeroPadded) {
  DECL_T();

  int64_t size = 10000;
  this->RandomData(size);

  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;

  // first slug
  int64_t K = 1000;

  ASSERT_OK(this->builder_->AppendValues(draws.data(), K, valid_bytes.data()));

  std::shared_ptr<Array> out;
  FinishAndCheckPadding(this->builder_.get(), &out);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValuesStdBool) {
  // ARROW-1383
  DECL_T();

  int64_t size = 10000;
  this->RandomData(size);

  vector<T>& draws = this->draws_;

  std::vector<bool> is_valid;

  // first slug
  int64_t K = 1000;

  for (int64_t i = 0; i < K; ++i) {
    is_valid.push_back(this->valid_bytes_[i] != 0);
  }
  ASSERT_OK(this->builder_->AppendValues(draws.data(), K, is_valid));
  ASSERT_OK(this->builder_nn_->AppendValues(draws.data(), K));

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1024, this->builder_->capacity());
  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1024, this->builder_nn_->capacity());

  // Append the next 9000
  is_valid.clear();
  std::vector<T> partial_draws;
  for (int64_t i = K; i < size; ++i) {
    partial_draws.push_back(draws[i]);
    is_valid.push_back(this->valid_bytes_[i] != 0);
  }

  ASSERT_OK(this->builder_->AppendValues(partial_draws, is_valid));
  ASSERT_OK(this->builder_nn_->AppendValues(partial_draws));

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_->capacity());

  ASSERT_EQ(size, this->builder_nn_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAdvance) {
  int64_t n = 1000;
  ASSERT_OK(this->builder_->Reserve(n));

  ASSERT_OK(this->builder_->Advance(100));
  ASSERT_EQ(100, this->builder_->length());

  ASSERT_OK(this->builder_->Advance(900));

  int64_t too_many = this->builder_->capacity() - 1000 + 1;
  ASSERT_RAISES(Invalid, this->builder_->Advance(too_many));
}

TYPED_TEST(TestPrimitiveBuilder, TestResize) {
  int64_t cap = kMinBuilderCapacity * 2;

  ASSERT_OK(this->builder_->Reserve(cap));
  ASSERT_EQ(cap, this->builder_->capacity());
}

TYPED_TEST(TestPrimitiveBuilder, TestReserve) {
  ASSERT_OK(this->builder_->Reserve(10));
  ASSERT_EQ(0, this->builder_->length());
  ASSERT_EQ(kMinBuilderCapacity, this->builder_->capacity());

  ASSERT_OK(this->builder_->Reserve(90));
  ASSERT_OK(this->builder_->Advance(100));
  ASSERT_OK(this->builder_->Reserve(kMinBuilderCapacity));

  ASSERT_RAISES(Invalid, this->builder_->Resize(1));

  ASSERT_EQ(BitUtil::NextPower2(kMinBuilderCapacity + 100), this->builder_->capacity());
}

TEST(TestBooleanBuilder, AppendNullsAdvanceBuilder) {
  BooleanBuilder builder;

  std::vector<uint8_t> values = {1, 0, 0, 1};
  std::vector<uint8_t> is_valid = {1, 1, 0, 1};

  std::shared_ptr<Array> arr;
  ASSERT_OK(builder.AppendValues(values.data(), 2));
  ASSERT_OK(builder.AppendNulls(1));
  ASSERT_OK(builder.AppendValues(values.data() + 3, 1));
  ASSERT_OK(builder.Finish(&arr));

  ASSERT_EQ(1, arr->null_count());

  const auto& barr = static_cast<const BooleanArray&>(*arr);
  ASSERT_TRUE(barr.Value(0));
  ASSERT_FALSE(barr.Value(1));
  ASSERT_TRUE(barr.IsNull(2));
  ASSERT_TRUE(barr.Value(3));
}

TEST(TestBooleanBuilder, TestStdBoolVectorAppend) {
  BooleanBuilder builder;
  BooleanBuilder builder_nn;

  std::vector<bool> values, is_valid;

  const int length = 10000;
  random_is_valid(length, 0.5, &values);
  random_is_valid(length, 0.1, &is_valid);

  const int chunksize = 1000;
  for (int chunk = 0; chunk < length / chunksize; ++chunk) {
    std::vector<bool> chunk_values, chunk_is_valid;
    for (int i = chunk * chunksize; i < (chunk + 1) * chunksize; ++i) {
      chunk_values.push_back(values[i]);
      chunk_is_valid.push_back(is_valid[i]);
    }
    ASSERT_OK(builder.AppendValues(chunk_values, chunk_is_valid));
    ASSERT_OK(builder_nn.AppendValues(chunk_values));
  }

  std::shared_ptr<Array> result, result_nn;
  ASSERT_OK(builder.Finish(&result));
  ASSERT_OK(builder_nn.Finish(&result_nn));

  const auto& arr = checked_cast<const BooleanArray&>(*result);
  const auto& arr_nn = checked_cast<const BooleanArray&>(*result_nn);
  for (int i = 0; i < length; ++i) {
    if (is_valid[i]) {
      ASSERT_FALSE(arr.IsNull(i));
      ASSERT_EQ(values[i], arr.Value(i));
    } else {
      ASSERT_TRUE(arr.IsNull(i));
    }
    ASSERT_EQ(values[i], arr_nn.Value(i));
  }
}

template <typename TYPE>
void CheckSliceApproxEquals() {
  using T = typename TYPE::c_type;

  const int64_t kSize = 50;
  vector<T> draws1;
  vector<T> draws2;

  const uint32_t kSeed = 0;
  random_real(kSize, kSeed, 0.0, 100.0, &draws1);
  random_real(kSize, kSeed + 1, 0.0, 100.0, &draws2);

  // Make the draws equal in the sliced segment, but unequal elsewhere (to
  // catch not using the slice offset)
  for (int64_t i = 10; i < 30; ++i) {
    draws2[i] = draws1[i];
  }

  vector<bool> is_valid;
  random_is_valid(kSize, 0.1, &is_valid);

  std::shared_ptr<Array> array1, array2;
  ArrayFromVector<TYPE, T>(is_valid, draws1, &array1);
  ArrayFromVector<TYPE, T>(is_valid, draws2, &array2);

  std::shared_ptr<Array> slice1 = array1->Slice(10, 20);
  std::shared_ptr<Array> slice2 = array2->Slice(10, 20);

  ASSERT_TRUE(slice1->ApproxEquals(slice2));
}

TEST(TestPrimitiveAdHoc, FloatingSliceApproxEquals) {
  CheckSliceApproxEquals<FloatType>();
  CheckSliceApproxEquals<DoubleType>();
}

// ----------------------------------------------------------------------
// FixedSizeBinary tests

class TestFWBinaryArray : public ::testing::Test {
 public:
  void SetUp() {}

  void InitBuilder(int byte_width) {
    auto type = fixed_size_binary(byte_width);
    builder_.reset(new FixedSizeBinaryBuilder(type, default_memory_pool()));
  }

 protected:
  std::unique_ptr<FixedSizeBinaryBuilder> builder_;
};

TEST_F(TestFWBinaryArray, Builder) {
  int32_t byte_width = 10;
  int64_t length = 4096;

  int64_t nbytes = length * byte_width;

  vector<uint8_t> data(nbytes);
  random_bytes(nbytes, 0, data.data());

  vector<uint8_t> is_valid(length);
  random_null_bytes(length, 0.1, is_valid.data());

  const uint8_t* raw_data = data.data();

  std::shared_ptr<Array> result;

  auto CheckResult = [&length, &is_valid, &raw_data, &byte_width](const Array& result) {
    // Verify output
    const auto& fw_result = checked_cast<const FixedSizeBinaryArray&>(result);

    ASSERT_EQ(length, result.length());

    for (int64_t i = 0; i < result.length(); ++i) {
      if (is_valid[i]) {
        ASSERT_EQ(0,
                  memcmp(raw_data + byte_width * i, fw_result.GetValue(i), byte_width));
      } else {
        ASSERT_TRUE(fw_result.IsNull(i));
      }
    }
  };

  // Build using iterative API
  InitBuilder(byte_width);
  for (int64_t i = 0; i < length; ++i) {
    if (is_valid[i]) {
      ASSERT_OK(builder_->Append(raw_data + byte_width * i));
    } else {
      ASSERT_OK(builder_->AppendNull());
    }
  }

  FinishAndCheckPadding(builder_.get(), &result);
  CheckResult(*result);

  // Build using batch API
  InitBuilder(byte_width);

  const uint8_t* raw_is_valid = is_valid.data();

  ASSERT_OK(builder_->AppendValues(raw_data, 50, raw_is_valid));
  ASSERT_OK(
      builder_->AppendValues(raw_data + 50 * byte_width, length - 50, raw_is_valid + 50));
  FinishAndCheckPadding(builder_.get(), &result);

  CheckResult(*result);

  // Build from std::string
  InitBuilder(byte_width);
  for (int64_t i = 0; i < length; ++i) {
    if (is_valid[i]) {
      ASSERT_OK(builder_->Append(
          string(reinterpret_cast<const char*>(raw_data + byte_width * i), byte_width)));
    } else {
      ASSERT_OK(builder_->AppendNull());
    }
  }

  ASSERT_OK(builder_->Finish(&result));
  CheckResult(*result);
}

TEST_F(TestFWBinaryArray, EqualsRangeEquals) {
  // Check that we don't compare data in null slots

  auto type = fixed_size_binary(4);
  FixedSizeBinaryBuilder builder1(type);
  FixedSizeBinaryBuilder builder2(type);

  ASSERT_OK(builder1.Append("foo1"));
  ASSERT_OK(builder1.AppendNull());

  ASSERT_OK(builder2.Append("foo1"));
  ASSERT_OK(builder2.Append("foo2"));

  std::shared_ptr<Array> array1, array2;
  ASSERT_OK(builder1.Finish(&array1));
  ASSERT_OK(builder2.Finish(&array2));

  const auto& a1 = checked_cast<const FixedSizeBinaryArray&>(*array1);
  const auto& a2 = checked_cast<const FixedSizeBinaryArray&>(*array2);

  FixedSizeBinaryArray equal1(type, 2, a1.values(), a1.null_bitmap(), 1);
  FixedSizeBinaryArray equal2(type, 2, a2.values(), a1.null_bitmap(), 1);

  ASSERT_TRUE(equal1.Equals(equal2));
  ASSERT_TRUE(equal1.RangeEquals(equal2, 0, 2, 0));
}

TEST_F(TestFWBinaryArray, ZeroSize) {
  auto type = fixed_size_binary(0);
  FixedSizeBinaryBuilder builder(type);

  ASSERT_OK(builder.Append(""));
  ASSERT_OK(builder.Append(std::string()));
  ASSERT_OK(builder.Append(static_cast<const uint8_t*>(nullptr)));
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.AppendNull());

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  const auto& fw_array = checked_cast<const FixedSizeBinaryArray&>(*array);

  // data is never allocated
  ASSERT_TRUE(fw_array.values() == nullptr);
  ASSERT_EQ(0, fw_array.byte_width());

  ASSERT_EQ(6, array->length());
  ASSERT_EQ(3, array->null_count());
}

TEST_F(TestFWBinaryArray, ZeroPadding) {
  auto type = fixed_size_binary(4);
  FixedSizeBinaryBuilder builder(type);

  ASSERT_OK(builder.Append("foo1"));
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Append("foo2"));
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Append("foo3"));

  std::shared_ptr<Array> array;
  FinishAndCheckPadding(&builder, &array);
}

TEST_F(TestFWBinaryArray, Slice) {
  auto type = fixed_size_binary(4);
  FixedSizeBinaryBuilder builder(type);

  vector<string> strings = {"foo1", "foo2", "foo3", "foo4", "foo5"};
  vector<uint8_t> is_null = {0, 1, 0, 0, 0};

  for (int i = 0; i < 5; ++i) {
    if (is_null[i]) {
      ASSERT_OK(builder.AppendNull());
    } else {
      ASSERT_OK(builder.Append(strings[i]));
    }
  }

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  std::shared_ptr<Array> slice, slice2;

  slice = array->Slice(1);
  slice2 = array->Slice(1);
  ASSERT_EQ(4, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(1, slice->length(), 0, slice));

  // Chained slices
  slice = array->Slice(2);
  slice2 = array->Slice(1)->Slice(1);
  ASSERT_TRUE(slice->Equals(slice2));

  slice = array->Slice(1, 3);
  ASSERT_EQ(3, slice->length());

  slice2 = array->Slice(1, 3);
  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(1, 3, 0, slice));
}

// ----------------------------------------------------------------------
// AdaptiveInt tests

class TestAdaptiveIntBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();
    builder_ = std::make_shared<AdaptiveIntBuilder>(pool_);
  }

  void Done() { FinishAndCheckPadding(builder_.get(), &result_); }

 protected:
  std::shared_ptr<AdaptiveIntBuilder> builder_;

  std::shared_ptr<Array> expected_;
  std::shared_ptr<Array> result_;
};

TEST_F(TestAdaptiveIntBuilder, TestInt8) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_OK(builder_->Append(127));
  ASSERT_OK(builder_->Append(-128));

  Done();

  std::vector<int8_t> expected_values({0, 127, -128});
  ArrayFromVector<Int8Type, int8_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestInt16) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_OK(builder_->Append(128));
  Done();

  std::vector<int16_t> expected_values({0, 128});
  ArrayFromVector<Int16Type, int16_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(builder_->Append(-129));
  expected_values = {-129};
  Done();

  ArrayFromVector<Int16Type, int16_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<int16_t>::max()));
  ASSERT_OK(builder_->Append(std::numeric_limits<int16_t>::min()));
  expected_values = {std::numeric_limits<int16_t>::max(),
                     std::numeric_limits<int16_t>::min()};
  Done();

  ArrayFromVector<Int16Type, int16_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestInt32) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_OK(
      builder_->Append(static_cast<int64_t>(std::numeric_limits<int16_t>::max()) + 1));
  Done();

  std::vector<int32_t> expected_values(
      {0, static_cast<int32_t>(std::numeric_limits<int16_t>::max()) + 1});
  ArrayFromVector<Int32Type, int32_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(
      builder_->Append(static_cast<int64_t>(std::numeric_limits<int16_t>::min()) - 1));
  expected_values = {static_cast<int32_t>(std::numeric_limits<int16_t>::min()) - 1};
  Done();

  ArrayFromVector<Int32Type, int32_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<int32_t>::max()));
  ASSERT_OK(builder_->Append(std::numeric_limits<int32_t>::min()));
  expected_values = {std::numeric_limits<int32_t>::max(),
                     std::numeric_limits<int32_t>::min()};
  Done();

  ArrayFromVector<Int32Type, int32_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestInt64) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_OK(
      builder_->Append(static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1));
  Done();

  std::vector<int64_t> expected_values(
      {0, static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1});
  ArrayFromVector<Int64Type, int64_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(
      builder_->Append(static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1));
  expected_values = {static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1};
  Done();

  ArrayFromVector<Int64Type, int64_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<int64_t>::max()));
  ASSERT_OK(builder_->Append(std::numeric_limits<int64_t>::min()));
  expected_values = {std::numeric_limits<int64_t>::max(),
                     std::numeric_limits<int64_t>::min()};
  Done();

  ArrayFromVector<Int64Type, int64_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestAppendValues) {
  {
    std::vector<int64_t> expected_values(
        {0, static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1});
    ASSERT_OK(builder_->AppendValues(expected_values.data(), expected_values.size()));
    Done();

    ArrayFromVector<Int64Type, int64_t>(expected_values, &expected_);
    AssertArraysEqual(*expected_, *result_);
  }
  {
    SetUp();
    std::vector<int64_t> values(
        {0, std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()});
    ASSERT_OK(builder_->AppendValues(values.data(), values.size()));
    Done();

    std::vector<int32_t> expected_values(
        {0, std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()});

    ArrayFromVector<Int32Type, int32_t>(expected_values, &expected_);
    AssertArraysEqual(*expected_, *result_);
  }
  {
    SetUp();
    std::vector<int64_t> values(
        {0, std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max()});
    ASSERT_OK(builder_->AppendValues(values.data(), values.size()));
    Done();

    std::vector<int16_t> expected_values(
        {0, std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max()});

    ArrayFromVector<Int16Type, int16_t>(expected_values, &expected_);
    AssertArraysEqual(*expected_, *result_);
  }
  {
    SetUp();
    std::vector<int64_t> values(
        {0, std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max()});
    ASSERT_OK(builder_->AppendValues(values.data(), values.size()));
    Done();

    std::vector<int8_t> expected_values(
        {0, std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max()});

    ArrayFromVector<Int8Type, int8_t>(expected_values, &expected_);
    AssertArraysEqual(*expected_, *result_);
  }
}

TEST_F(TestAdaptiveIntBuilder, TestAssertZeroPadded) {
  std::vector<int64_t> values(
      {0, static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1});
  ASSERT_OK(builder_->AppendValues(values.data(), values.size()));
  Done();
}

TEST_F(TestAdaptiveIntBuilder, TestAppendNull) {
  int64_t size = 1000;
  ASSERT_OK(builder_->Append(127));
  for (unsigned index = 1; index < size - 1; ++index) {
    ASSERT_OK(builder_->AppendNull());
  }
  ASSERT_OK(builder_->Append(-128));

  Done();

  std::vector<bool> expected_valid(size, false);
  expected_valid[0] = true;
  expected_valid[size - 1] = true;
  std::vector<int8_t> expected_values(size);
  expected_values[0] = 127;
  expected_values[size - 1] = -128;
  std::shared_ptr<Array> expected;
  ArrayFromVector<Int8Type, int8_t>(expected_valid, expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestAppendNulls) {
  constexpr int64_t size = 10;
  ASSERT_OK(builder_->AppendNulls(size));

  Done();

  for (unsigned index = 0; index < size; ++index) {
    ASSERT_FALSE(result_->IsValid(index));
  }
}

class TestAdaptiveUIntBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();
    builder_ = std::make_shared<AdaptiveUIntBuilder>(pool_);
  }

  void Done() { FinishAndCheckPadding(builder_.get(), &result_); }

 protected:
  std::shared_ptr<AdaptiveUIntBuilder> builder_;

  std::shared_ptr<Array> expected_;
  std::shared_ptr<Array> result_;
};

TEST_F(TestAdaptiveUIntBuilder, TestUInt8) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_OK(builder_->Append(255));

  Done();

  std::vector<uint8_t> expected_values({0, 255});
  ArrayFromVector<UInt8Type, uint8_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));
}

TEST_F(TestAdaptiveUIntBuilder, TestUInt16) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_OK(builder_->Append(256));
  Done();

  std::vector<uint16_t> expected_values({0, 256});
  ArrayFromVector<UInt16Type, uint16_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<uint16_t>::max()));
  expected_values = {std::numeric_limits<uint16_t>::max()};
  Done();

  ArrayFromVector<UInt16Type, uint16_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));
}

TEST_F(TestAdaptiveUIntBuilder, TestUInt32) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_OK(
      builder_->Append(static_cast<uint64_t>(std::numeric_limits<uint16_t>::max()) + 1));
  Done();

  std::vector<uint32_t> expected_values(
      {0, static_cast<uint32_t>(std::numeric_limits<uint16_t>::max()) + 1});
  ArrayFromVector<UInt32Type, uint32_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<uint32_t>::max()));
  expected_values = {std::numeric_limits<uint32_t>::max()};
  Done();

  ArrayFromVector<UInt32Type, uint32_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));
}

TEST_F(TestAdaptiveUIntBuilder, TestUInt64) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_OK(
      builder_->Append(static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1));
  Done();

  std::vector<uint64_t> expected_values(
      {0, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1});
  ArrayFromVector<UInt64Type, uint64_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<uint64_t>::max()));
  expected_values = {std::numeric_limits<uint64_t>::max()};
  Done();

  ArrayFromVector<UInt64Type, uint64_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));
}

TEST_F(TestAdaptiveUIntBuilder, TestAppendValues) {
  std::vector<uint64_t> expected_values(
      {0, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1});
  ASSERT_OK(builder_->AppendValues(expected_values.data(), expected_values.size()));
  Done();

  ArrayFromVector<UInt64Type, uint64_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));
}

TEST_F(TestAdaptiveUIntBuilder, TestAssertZeroPadded) {
  std::vector<uint64_t> values(
      {0, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1});
  ASSERT_OK(builder_->AppendValues(values.data(), values.size()));
  Done();
}

TEST_F(TestAdaptiveUIntBuilder, TestAppendNull) {
  int64_t size = 1000;
  ASSERT_OK(builder_->Append(254));
  for (unsigned index = 1; index < size - 1; ++index) {
    ASSERT_OK(builder_->AppendNull());
  }
  ASSERT_OK(builder_->Append(255));

  Done();

  std::vector<bool> expected_valid(size, false);
  expected_valid[0] = true;
  expected_valid[size - 1] = true;
  std::vector<uint8_t> expected_values(size);
  expected_values[0] = 254;
  expected_values[size - 1] = 255;
  std::shared_ptr<Array> expected;
  ArrayFromVector<UInt8Type, uint8_t>(expected_valid, expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveUIntBuilder, TestAppendNulls) {
  constexpr int64_t size = 10;
  ASSERT_OK(builder_->AppendNulls(size));

  Done();

  for (unsigned index = 0; index < size; ++index) {
    ASSERT_FALSE(result_->IsValid(index));
  }
}

// ----------------------------------------------------------------------
// Union tests

TEST(TestUnionArrayAdHoc, TestSliceEquals) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(ipc::MakeUnion(&batch));

  const int64_t size = batch->num_rows();

  auto CheckUnion = [&size](std::shared_ptr<Array> array) {
    std::shared_ptr<Array> slice, slice2;
    slice = array->Slice(2);
    ASSERT_EQ(size - 2, slice->length());

    slice2 = array->Slice(2);
    ASSERT_EQ(size - 2, slice->length());

    ASSERT_TRUE(slice->Equals(slice2));
    ASSERT_TRUE(array->RangeEquals(2, array->length(), 0, slice));

    // Chained slices
    slice2 = array->Slice(1)->Slice(1);
    ASSERT_TRUE(slice->Equals(slice2));

    slice = array->Slice(1, 5);
    slice2 = array->Slice(1, 5);
    ASSERT_EQ(5, slice->length());

    ASSERT_TRUE(slice->Equals(slice2));
    ASSERT_TRUE(array->RangeEquals(1, 6, 0, slice));

    AssertZeroPadded(*array);
    TestInitialized(*array);
  };

  CheckUnion(batch->column(1));
  CheckUnion(batch->column(2));
}

using DecimalVector = std::vector<Decimal128>;

class DecimalTest : public ::testing::TestWithParam<int> {
 public:
  DecimalTest() {}

  template <size_t BYTE_WIDTH = 16>
  void MakeData(const DecimalVector& input, std::vector<uint8_t>* out) const {
    out->reserve(input.size() * BYTE_WIDTH);

    for (const auto& value : input) {
      auto bytes = value.ToBytes();
      out->insert(out->end(), bytes.cbegin(), bytes.cend());
    }
  }

  template <size_t BYTE_WIDTH = 16>
  void TestCreate(int32_t precision, const DecimalVector& draw,
                  const std::vector<uint8_t>& valid_bytes, int64_t offset) const {
    auto type = std::make_shared<Decimal128Type>(precision, 4);
    auto builder = std::make_shared<Decimal128Builder>(type);

    size_t null_count = 0;

    const size_t size = draw.size();

    ASSERT_OK(builder->Reserve(size));

    for (size_t i = 0; i < size; ++i) {
      if (valid_bytes[i]) {
        ASSERT_OK(builder->Append(draw[i]));
      } else {
        ASSERT_OK(builder->AppendNull());
        ++null_count;
      }
    }

    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder.get(), &out);

    std::vector<uint8_t> raw_bytes;

    raw_bytes.reserve(size * BYTE_WIDTH);
    MakeData<BYTE_WIDTH>(draw, &raw_bytes);

    auto expected_data = std::make_shared<Buffer>(raw_bytes.data(), BYTE_WIDTH);
    std::shared_ptr<Buffer> expected_null_bitmap;
    ASSERT_OK(
        BitUtil::BytesToBits(valid_bytes, default_memory_pool(), &expected_null_bitmap));

    int64_t expected_null_count = CountNulls(valid_bytes);
    auto expected = std::make_shared<Decimal128Array>(
        type, size, expected_data, expected_null_bitmap, expected_null_count);

    std::shared_ptr<Array> lhs = out->Slice(offset);
    std::shared_ptr<Array> rhs = expected->Slice(offset);
    ASSERT_TRUE(lhs->Equals(rhs));
  }
};

TEST_P(DecimalTest, NoNulls) {
  int32_t precision = GetParam();
  std::vector<Decimal128> draw = {Decimal128(1), Decimal128(-2), Decimal128(2389),
                                  Decimal128(4), Decimal128(-12348)};
  std::vector<uint8_t> valid_bytes = {true, true, true, true, true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

TEST_P(DecimalTest, WithNulls) {
  int32_t precision = GetParam();
  std::vector<Decimal128> draw = {Decimal128(1), Decimal128(2),  Decimal128(-1),
                                  Decimal128(4), Decimal128(-1), Decimal128(1),
                                  Decimal128(2)};
  Decimal128 big;
  ASSERT_OK(Decimal128::FromString("230342903942.234234", &big));
  draw.push_back(big);

  Decimal128 big_negative;
  ASSERT_OK(Decimal128::FromString("-23049302932.235234", &big_negative));
  draw.push_back(big_negative);

  std::vector<uint8_t> valid_bytes = {true, true, false, true, false,
                                      true, true, true,  true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

INSTANTIATE_TEST_CASE_P(DecimalTest, DecimalTest, ::testing::Range(1, 38));

// ----------------------------------------------------------------------
// Test rechunking

TEST(TestRechunkArraysConsistently, Trivial) {
  std::vector<ArrayVector> groups, rechunked;
  rechunked = internal::RechunkArraysConsistently(groups);
  ASSERT_EQ(rechunked.size(), 0);

  std::shared_ptr<Array> a1, a2, b1;
  ArrayFromVector<Int16Type, int16_t>({}, &a1);
  ArrayFromVector<Int16Type, int16_t>({}, &a2);
  ArrayFromVector<Int32Type, int32_t>({}, &b1);

  groups = {{a1, a2}, {}, {b1}};
  rechunked = internal::RechunkArraysConsistently(groups);
  ASSERT_EQ(rechunked.size(), 3);

  for (auto& arrvec : rechunked) {
    for (auto& arr : arrvec) {
      AssertZeroPadded(*arr);
      TestInitialized(*arr);
    }
  }
}

TEST(TestRechunkArraysConsistently, Plain) {
  std::shared_ptr<Array> expected;
  std::shared_ptr<Array> a1, a2, a3, b1, b2, b3, b4;
  ArrayFromVector<Int16Type, int16_t>({1, 2, 3}, &a1);
  ArrayFromVector<Int16Type, int16_t>({4, 5}, &a2);
  ArrayFromVector<Int16Type, int16_t>({6, 7, 8, 9}, &a3);

  ArrayFromVector<Int32Type, int32_t>({41, 42}, &b1);
  ArrayFromVector<Int32Type, int32_t>({43, 44, 45}, &b2);
  ArrayFromVector<Int32Type, int32_t>({46, 47}, &b3);
  ArrayFromVector<Int32Type, int32_t>({48, 49}, &b4);

  ArrayVector a{a1, a2, a3};
  ArrayVector b{b1, b2, b3, b4};

  std::vector<ArrayVector> groups{a, b}, rechunked;
  rechunked = internal::RechunkArraysConsistently(groups);
  ASSERT_EQ(rechunked.size(), 2);
  auto ra = rechunked[0];
  auto rb = rechunked[1];

  ASSERT_EQ(ra.size(), 5);
  ArrayFromVector<Int16Type, int16_t>({1, 2}, &expected);
  ASSERT_ARRAYS_EQUAL(*ra[0], *expected);
  ArrayFromVector<Int16Type, int16_t>({3}, &expected);
  ASSERT_ARRAYS_EQUAL(*ra[1], *expected);
  ArrayFromVector<Int16Type, int16_t>({4, 5}, &expected);
  ASSERT_ARRAYS_EQUAL(*ra[2], *expected);
  ArrayFromVector<Int16Type, int16_t>({6, 7}, &expected);
  ASSERT_ARRAYS_EQUAL(*ra[3], *expected);
  ArrayFromVector<Int16Type, int16_t>({8, 9}, &expected);
  ASSERT_ARRAYS_EQUAL(*ra[4], *expected);

  ASSERT_EQ(rb.size(), 5);
  ArrayFromVector<Int32Type, int32_t>({41, 42}, &expected);
  ASSERT_ARRAYS_EQUAL(*rb[0], *expected);
  ArrayFromVector<Int32Type, int32_t>({43}, &expected);
  ASSERT_ARRAYS_EQUAL(*rb[1], *expected);
  ArrayFromVector<Int32Type, int32_t>({44, 45}, &expected);
  ASSERT_ARRAYS_EQUAL(*rb[2], *expected);
  ArrayFromVector<Int32Type, int32_t>({46, 47}, &expected);
  ASSERT_ARRAYS_EQUAL(*rb[3], *expected);
  ArrayFromVector<Int32Type, int32_t>({48, 49}, &expected);
  ASSERT_ARRAYS_EQUAL(*rb[4], *expected);

  for (auto& arrvec : rechunked) {
    for (auto& arr : arrvec) {
      AssertZeroPadded(*arr);
      TestInitialized(*arr);
    }
  }
}

}  // namespace arrow
