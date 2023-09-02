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

#include <stddef.h>
#include <stdlib.h>

#include "duckdb/common/arrow/nanoarrow/nanoarrow.hpp"

namespace duckdb_nanoarrow {

void *ArrowMalloc(int64_t size) {
	return malloc(size);
}

void *ArrowRealloc(void *ptr, int64_t size) {
	return realloc(ptr, size);
}

void ArrowFree(void *ptr) {
	free(ptr);
}

static uint8_t *ArrowBufferAllocatorMallocAllocate(struct ArrowBufferAllocator *allocator, int64_t size) {
	return (uint8_t *)ArrowMalloc(size);
}

static uint8_t *ArrowBufferAllocatorMallocReallocate(struct ArrowBufferAllocator *allocator, uint8_t *ptr,
                                                     int64_t old_size, int64_t new_size) {
	return (uint8_t *)ArrowRealloc(ptr, new_size);
}

static void ArrowBufferAllocatorMallocFree(struct ArrowBufferAllocator *allocator, uint8_t *ptr, int64_t size) {
	ArrowFree(ptr);
}

static struct ArrowBufferAllocator ArrowBufferAllocatorMalloc = {
    &ArrowBufferAllocatorMallocAllocate, &ArrowBufferAllocatorMallocReallocate, &ArrowBufferAllocatorMallocFree, NULL};

struct ArrowBufferAllocator *ArrowBufferAllocatorDefault() {
	return &ArrowBufferAllocatorMalloc;
}

} // namespace duckdb_nanoarrow
