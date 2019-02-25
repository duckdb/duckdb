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
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer-builder.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"

using std::string;

namespace arrow {

TEST(TestAllocate, Bitmap) {
  std::shared_ptr<Buffer> new_buffer;
  EXPECT_OK(AllocateBitmap(default_memory_pool(), 100, &new_buffer));
  EXPECT_GE(new_buffer->size(), 13);
  EXPECT_EQ(new_buffer->capacity() % 8, 0);
}

TEST(TestAllocate, EmptyBitmap) {
  std::shared_ptr<Buffer> new_buffer;
  EXPECT_OK(AllocateEmptyBitmap(default_memory_pool(), 100, &new_buffer));
  EXPECT_EQ(new_buffer->size(), 13);
  EXPECT_EQ(new_buffer->capacity() % 8, 0);
  EXPECT_TRUE(std::all_of(new_buffer->data(), new_buffer->data() + new_buffer->capacity(),
                          [](int8_t byte) { return byte == 0; }));
}

TEST(TestBuffer, FromStdString) {
  std::string val = "hello, world";

  Buffer buf(val);
  ASSERT_EQ(0, memcmp(buf.data(), val.c_str(), val.size()));
  ASSERT_EQ(static_cast<int64_t>(val.size()), buf.size());
}

TEST(TestBuffer, FromStdStringWithMemory) {
  std::string expected = "hello, world";
  std::shared_ptr<Buffer> buf;

  {
    std::string temp = "hello, world";
    ASSERT_OK(Buffer::FromString(temp, &buf));
    ASSERT_EQ(0, memcmp(buf->data(), temp.c_str(), temp.size()));
    ASSERT_EQ(static_cast<int64_t>(temp.size()), buf->size());
  }

  // Now temp goes out of scope and we check if created buffer
  // is still valid to make sure it actually owns its space
  ASSERT_EQ(0, memcmp(buf->data(), expected.c_str(), expected.size()));
  ASSERT_EQ(static_cast<int64_t>(expected.size()), buf->size());
}

TEST(TestBuffer, EqualsWithSameContent) {
  MemoryPool* pool = default_memory_pool();
  const int32_t bufferSize = 128 * 1024;
  uint8_t* rawBuffer1;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer1));
  memset(rawBuffer1, 12, bufferSize);
  uint8_t* rawBuffer2;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer2));
  memset(rawBuffer2, 12, bufferSize);
  uint8_t* rawBuffer3;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer3));
  memset(rawBuffer3, 3, bufferSize);

  Buffer buffer1(rawBuffer1, bufferSize);
  Buffer buffer2(rawBuffer2, bufferSize);
  Buffer buffer3(rawBuffer3, bufferSize);
  ASSERT_TRUE(buffer1.Equals(buffer2));
  ASSERT_FALSE(buffer1.Equals(buffer3));

  pool->Free(rawBuffer1, bufferSize);
  pool->Free(rawBuffer2, bufferSize);
  pool->Free(rawBuffer3, bufferSize);
}

TEST(TestBuffer, EqualsWithSameBuffer) {
  MemoryPool* pool = default_memory_pool();
  const int32_t bufferSize = 128 * 1024;
  uint8_t* rawBuffer;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer));
  memset(rawBuffer, 111, bufferSize);

  Buffer buffer1(rawBuffer, bufferSize);
  Buffer buffer2(rawBuffer, bufferSize);
  ASSERT_TRUE(buffer1.Equals(buffer2));

  const int64_t nbytes = bufferSize / 2;
  Buffer buffer3(rawBuffer, nbytes);
  ASSERT_TRUE(buffer1.Equals(buffer3, nbytes));
  ASSERT_FALSE(buffer1.Equals(buffer3, nbytes + 1));

  pool->Free(rawBuffer, bufferSize);
}

TEST(TestBuffer, Copy) {
  std::string data_str = "some data to copy";

  auto data = reinterpret_cast<const uint8_t*>(data_str.c_str());

  Buffer buf(data, data_str.size());

  std::shared_ptr<Buffer> out;

  ASSERT_OK(buf.Copy(5, 4, &out));

  Buffer expected(data + 5, 4);
  ASSERT_TRUE(out->Equals(expected));
  // assert the padding is zeroed
  std::vector<uint8_t> zeros(out->capacity() - out->size());
  ASSERT_EQ(0, memcmp(out->data() + out->size(), zeros.data(), zeros.size()));
}

TEST(TestBuffer, SliceBuffer) {
  std::string data_str = "some data to slice";

  auto data = reinterpret_cast<const uint8_t*>(data_str.c_str());

  auto buf = std::make_shared<Buffer>(data, data_str.size());

  std::shared_ptr<Buffer> out = SliceBuffer(buf, 5, 4);
  Buffer expected(data + 5, 4);
  ASSERT_TRUE(out->Equals(expected));

  ASSERT_EQ(2, buf.use_count());
}

TEST(TestMutableBuffer, Wrap) {
  std::vector<int32_t> values = {1, 2, 3};

  auto buf = MutableBuffer::Wrap(values.data(), values.size());
  reinterpret_cast<int32_t*>(buf->mutable_data())[1] = 4;

  ASSERT_EQ(4, values[1]);
}

TEST(TestBuffer, FromStringRvalue) {
  std::string expected = "input data";

  std::shared_ptr<Buffer> buffer;
  {
    std::string data_str = "input data";
    buffer = Buffer::FromString(std::move(data_str));
  }

  ASSERT_FALSE(buffer->is_mutable());

  ASSERT_EQ(0, memcmp(buffer->data(), expected.c_str(), expected.size()));
  ASSERT_EQ(static_cast<int64_t>(expected.size()), buffer->size());
}

TEST(TestBuffer, SliceMutableBuffer) {
  std::string data_str = "some data to slice";
  auto data = reinterpret_cast<const uint8_t*>(data_str.c_str());

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(AllocateBuffer(50, &buffer));

  memcpy(buffer->mutable_data(), data, data_str.size());

  std::shared_ptr<Buffer> slice = SliceMutableBuffer(buffer, 5, 10);
  ASSERT_TRUE(slice->is_mutable());
  ASSERT_EQ(10, slice->size());

  Buffer expected(data + 5, 10);
  ASSERT_TRUE(slice->Equals(expected));
}

template <typename AllocateFunction>
void TestZeroSizeAllocateBuffer(MemoryPool* pool, AllocateFunction&& allocate_func) {
  auto allocated_bytes = pool->bytes_allocated();
  {
    std::shared_ptr<Buffer> buffer;

    ASSERT_OK(allocate_func(pool, 0, &buffer));
    ASSERT_EQ(buffer->size(), 0);
    // Even 0-sized buffers should not have a null data pointer
    ASSERT_NE(buffer->data(), nullptr);
    ASSERT_EQ(buffer->mutable_data(), buffer->data());

    ASSERT_GE(pool->bytes_allocated(), allocated_bytes);
  }
  ASSERT_EQ(pool->bytes_allocated(), allocated_bytes);
}

TEST(TestAllocateBuffer, ZeroSize) {
  MemoryPool* pool = default_memory_pool();
  auto allocate_func = [](MemoryPool* pool, int64_t size, std::shared_ptr<Buffer>* out) {
    return AllocateBuffer(pool, size, out);
  };
  TestZeroSizeAllocateBuffer(pool, allocate_func);
}

TEST(TestAllocateResizableBuffer, ZeroSize) {
  MemoryPool* pool = default_memory_pool();
  auto allocate_func = [](MemoryPool* pool, int64_t size, std::shared_ptr<Buffer>* out) {
    std::shared_ptr<ResizableBuffer> res;
    RETURN_NOT_OK(AllocateResizableBuffer(pool, size, &res));
    *out = res;
    return Status::OK();
  };
  TestZeroSizeAllocateBuffer(pool, allocate_func);
}

TEST(TestAllocateResizableBuffer, ZeroResize) {
  MemoryPool* pool = default_memory_pool();
  auto allocated_bytes = pool->bytes_allocated();
  {
    std::shared_ptr<ResizableBuffer> buffer;

    ASSERT_OK(AllocateResizableBuffer(pool, 1000, &buffer));
    ASSERT_EQ(buffer->size(), 1000);
    ASSERT_NE(buffer->data(), nullptr);
    ASSERT_EQ(buffer->mutable_data(), buffer->data());

    ASSERT_GE(pool->bytes_allocated(), allocated_bytes + 1000);

    ASSERT_OK(buffer->Resize(0));
    ASSERT_NE(buffer->data(), nullptr);
    ASSERT_EQ(buffer->mutable_data(), buffer->data());

    ASSERT_GE(pool->bytes_allocated(), allocated_bytes);
    ASSERT_LT(pool->bytes_allocated(), allocated_bytes + 1000);
  }
  ASSERT_EQ(pool->bytes_allocated(), allocated_bytes);
}

TEST(TestBufferBuilder, ResizeReserve) {
  const std::string data = "some data";
  auto data_ptr = data.c_str();

  BufferBuilder builder;

  ASSERT_OK(builder.Append(data_ptr, 9));
  ASSERT_EQ(9, builder.length());

  ASSERT_OK(builder.Resize(128));
  ASSERT_EQ(128, builder.capacity());

  // Do not shrink to fit
  ASSERT_OK(builder.Resize(64, false));
  ASSERT_EQ(128, builder.capacity());

  // Shrink to fit
  ASSERT_OK(builder.Resize(64));
  ASSERT_EQ(64, builder.capacity());

  // Reserve elements
  ASSERT_OK(builder.Reserve(60));
  ASSERT_EQ(128, builder.capacity());
}

template <typename T>
class TypedTestBufferBuilder : public ::testing::Test {};

using BufferBuilderElements = ::testing::Types<int16_t, uint32_t, double>;

TYPED_TEST_CASE(TypedTestBufferBuilder, BufferBuilderElements);

TYPED_TEST(TypedTestBufferBuilder, BasicTypedBufferBuilderUsage) {
  TypedBufferBuilder<TypeParam> builder;

  ASSERT_OK(builder.Append(static_cast<TypeParam>(0)));
  ASSERT_EQ(builder.length(), 1);
  ASSERT_EQ(builder.capacity(), 64 / sizeof(TypeParam));

  constexpr int nvalues = 4;
  TypeParam values[nvalues];
  for (int i = 0; i != nvalues; ++i) {
    values[i] = static_cast<TypeParam>(i);
  }
  ASSERT_OK(builder.Append(values, nvalues));
  ASSERT_EQ(builder.length(), nvalues + 1);

  std::shared_ptr<Buffer> built;
  ASSERT_OK(builder.Finish(&built));

  auto data = reinterpret_cast<const TypeParam*>(built->data());
  ASSERT_EQ(data[0], static_cast<TypeParam>(0));
  for (auto value : values) {
    ++data;
    ASSERT_EQ(*data, value);
  }
}

TYPED_TEST(TypedTestBufferBuilder, AppendCopies) {
  TypedBufferBuilder<TypeParam> builder;

  ASSERT_OK(builder.Append(13, static_cast<TypeParam>(1)));
  ASSERT_OK(builder.Append(17, static_cast<TypeParam>(0)));
  ASSERT_EQ(builder.length(), 13 + 17);

  std::shared_ptr<Buffer> built;
  ASSERT_OK(builder.Finish(&built));

  auto data = reinterpret_cast<const TypeParam*>(built->data());
  for (int i = 0; i != 13 + 17; ++i, ++data) {
    ASSERT_EQ(*data, static_cast<TypeParam>(i < 13)) << "index = " << i;
  }
}

TEST(TestBufferBuilder, BasicBoolBufferBuilderUsage) {
  TypedBufferBuilder<bool> builder;

  ASSERT_OK(builder.Append(false));
  ASSERT_EQ(builder.length(), 1);
  ASSERT_EQ(builder.capacity(), 64 * 8);

  constexpr int nvalues = 4;
  uint8_t values[nvalues];
  for (int i = 0; i != nvalues; ++i) {
    values[i] = static_cast<uint8_t>(i);
  }
  ASSERT_OK(builder.Append(values, nvalues));
  ASSERT_EQ(builder.length(), nvalues + 1);

  ASSERT_EQ(builder.false_count(), 2);

  std::shared_ptr<Buffer> built;
  ASSERT_OK(builder.Finish(&built));

  ASSERT_EQ(BitUtil::GetBit(built->data(), 0), false);
  for (int i = 0; i != nvalues; ++i) {
    ASSERT_EQ(BitUtil::GetBit(built->data(), i + 1), static_cast<bool>(values[i]));
  }

  ASSERT_EQ(built->size(), BitUtil::BytesForBits(nvalues + 1));
}

TEST(TestBufferBuilder, BoolBufferBuilderAppendCopies) {
  TypedBufferBuilder<bool> builder;

  ASSERT_OK(builder.Append(13, true));
  ASSERT_OK(builder.Append(17, false));
  ASSERT_EQ(builder.length(), 13 + 17);
  ASSERT_EQ(builder.capacity(), 64 * 8);
  ASSERT_EQ(builder.false_count(), 17);

  std::shared_ptr<Buffer> built;
  ASSERT_OK(builder.Finish(&built));

  for (int i = 0; i != 13 + 17; ++i) {
    EXPECT_EQ(BitUtil::GetBit(built->data(), i), i < 13) << "index = " << i;
  }

  ASSERT_EQ(built->size(), BitUtil::BytesForBits(13 + 17));
}

template <typename T>
class TypedTestBuffer : public ::testing::Test {};

using BufferPtrs =
    ::testing::Types<std::shared_ptr<ResizableBuffer>, std::unique_ptr<ResizableBuffer>>;

TYPED_TEST_CASE(TypedTestBuffer, BufferPtrs);

TYPED_TEST(TypedTestBuffer, IsMutableFlag) {
  Buffer buf(nullptr, 0);

  ASSERT_FALSE(buf.is_mutable());

  MutableBuffer mbuf(nullptr, 0);
  ASSERT_TRUE(mbuf.is_mutable());

  TypeParam pool_buf;
  ASSERT_OK(AllocateResizableBuffer(0, &pool_buf));
  ASSERT_TRUE(pool_buf->is_mutable());
}

TYPED_TEST(TypedTestBuffer, Resize) {
  TypeParam buf;
  ASSERT_OK(AllocateResizableBuffer(0, &buf));

  ASSERT_EQ(0, buf->size());
  ASSERT_OK(buf->Resize(100));
  ASSERT_EQ(100, buf->size());
  ASSERT_OK(buf->Resize(200));
  ASSERT_EQ(200, buf->size());

  // Make it smaller, too
  ASSERT_OK(buf->Resize(50, true));
  ASSERT_EQ(50, buf->size());
  // We have actually shrunken in size
  // The spec requires that capacity is a multiple of 64
  ASSERT_EQ(64, buf->capacity());

  // Resize to a larger capacity again to test shrink_to_fit = false
  ASSERT_OK(buf->Resize(100));
  ASSERT_EQ(128, buf->capacity());
  ASSERT_OK(buf->Resize(50, false));
  ASSERT_EQ(128, buf->capacity());
}

TYPED_TEST(TypedTestBuffer, TypedResize) {
  TypeParam buf;
  ASSERT_OK(AllocateResizableBuffer(0, &buf));

  ASSERT_EQ(0, buf->size());
  ASSERT_OK(buf->template TypedResize<double>(100));
  ASSERT_EQ(800, buf->size());
  ASSERT_OK(buf->template TypedResize<double>(200));
  ASSERT_EQ(1600, buf->size());

  ASSERT_OK(buf->template TypedResize<double>(50, true));
  ASSERT_EQ(400, buf->size());
  ASSERT_EQ(448, buf->capacity());

  ASSERT_OK(buf->template TypedResize<double>(100));
  ASSERT_EQ(832, buf->capacity());
  ASSERT_OK(buf->template TypedResize<double>(50, false));
  ASSERT_EQ(832, buf->capacity());
}

TYPED_TEST(TypedTestBuffer, ResizeOOM) {
// This test doesn't play nice with AddressSanitizer
#ifndef ADDRESS_SANITIZER
  // realloc fails, even though there may be no explicit limit
  TypeParam buf;
  ASSERT_OK(AllocateResizableBuffer(0, &buf));
  ASSERT_OK(buf->Resize(100));
  int64_t to_alloc = std::min<uint64_t>(std::numeric_limits<int64_t>::max(),
                                        std::numeric_limits<size_t>::max());
  // subtract 63 to prevent overflow after the size is aligned
  to_alloc -= 63;
  ASSERT_RAISES(OutOfMemory, buf->Resize(to_alloc));
#endif
}

}  // namespace arrow
