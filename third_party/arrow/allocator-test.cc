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
#include <limits>
#include <memory>
#include <new>

#include <gtest/gtest.h>

#include "arrow/allocator.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

TEST(STLMemoryPool, Base) {
  std::allocator<uint8_t> allocator;
  STLMemoryPool<std::allocator<uint8_t>> pool(allocator);

  uint8_t* data = nullptr;
  ASSERT_OK(pool.Allocate(100, &data));
  ASSERT_EQ(pool.max_memory(), 100);
  ASSERT_EQ(pool.bytes_allocated(), 100);
  ASSERT_NE(data, nullptr);

  ASSERT_OK(pool.Reallocate(100, 150, &data));
  ASSERT_EQ(pool.max_memory(), 150);
  ASSERT_EQ(pool.bytes_allocated(), 150);

  pool.Free(data, 150);

  ASSERT_EQ(pool.max_memory(), 150);
  ASSERT_EQ(pool.bytes_allocated(), 0);
}

TEST(stl_allocator, MemoryTracking) {
  auto pool = default_memory_pool();
  stl_allocator<uint64_t> alloc;
  uint64_t* data = alloc.allocate(100);

  ASSERT_EQ(100 * sizeof(uint64_t), pool->bytes_allocated());

  alloc.deallocate(data, 100);
  ASSERT_EQ(0, pool->bytes_allocated());
}

#if !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER) || defined(ARROW_JEMALLOC))

TEST(stl_allocator, TestOOM) {
  stl_allocator<uint64_t> alloc;
  uint64_t to_alloc = std::numeric_limits<uint64_t>::max() / 2;
  ASSERT_THROW(alloc.allocate(to_alloc), std::bad_alloc);
}

TEST(stl_allocator, MaxMemory) {
  auto pool = default_memory_pool();

  stl_allocator<uint8_t> alloc(pool);
  uint8_t* data = alloc.allocate(1000);
  uint8_t* data2 = alloc.allocate(1000);

  alloc.deallocate(data, 1000);
  alloc.deallocate(data2, 1000);

  ASSERT_EQ(2000, pool->max_memory());
}

#endif  // ARROW_VALGRIND

}  // namespace arrow
