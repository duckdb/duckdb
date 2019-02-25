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

#include "arrow/memory_pool.h"

#include <algorithm>  // IWYU pragma: keep
#include <cstdlib>    // IWYU pragma: keep
#include <cstring>    // IWYU pragma: keep
#include <iostream>   // IWYU pragma: keep
#include <limits>
#include <memory>
#include <sstream>  // IWYU pragma: keep

#include "arrow/status.h"
#include "arrow/util/logging.h"  // IWYU pragma: keep

#ifdef ARROW_JEMALLOC
// Needed to support jemalloc 3 and 4
#define JEMALLOC_MANGLE
// Explicitly link to our version of jemalloc
#include "jemalloc_ep/dist/include/jemalloc/jemalloc.h"
#endif

namespace arrow {

constexpr size_t kAlignment = 64;

namespace {

// A static piece of memory for 0-size allocations, so as to return
// an aligned non-null pointer.
alignas(kAlignment) static uint8_t zero_size_area[1];

// Allocate memory according to the alignment requirements for Arrow
// (as of May 2016 64 bytes)
Status AllocateAligned(int64_t size, uint8_t** out) {
  // TODO(emkornfield) find something compatible with windows
  if (size < 0) {
    return Status::Invalid("negative malloc size");
  }
  if (size == 0) {
    *out = zero_size_area;
    return Status::OK();
  }
  if (static_cast<uint64_t>(size) >= std::numeric_limits<size_t>::max()) {
    return Status::CapacityError("malloc size overflows size_t");
  }
#ifdef _WIN32
  // Special code path for Windows
  *out =
      reinterpret_cast<uint8_t*>(_aligned_malloc(static_cast<size_t>(size), kAlignment));
  if (!*out) {
    return Status::OutOfMemory("malloc of size ", size, " failed");
  }
#elif defined(ARROW_JEMALLOC)
  *out = reinterpret_cast<uint8_t*>(
      mallocx(static_cast<size_t>(size), MALLOCX_ALIGN(kAlignment)));
  if (*out == NULL) {
    return Status::OutOfMemory("malloc of size ", size, " failed");
  }
#else
  const int result = posix_memalign(reinterpret_cast<void**>(out), kAlignment,
                                    static_cast<size_t>(size));
  if (result == ENOMEM) {
    return Status::OutOfMemory("malloc of size ", size, " failed");
  }

  if (result == EINVAL) {
    return Status::Invalid("invalid alignment parameter: ", kAlignment);
  }
#endif
  return Status::OK();
}

void DeallocateAligned(uint8_t* ptr, int64_t size) {
  if (ptr == zero_size_area) {
    DCHECK_EQ(size, 0);
  } else {
#ifdef _WIN32
    _aligned_free(ptr);
#elif defined(ARROW_JEMALLOC)
    dallocx(ptr, MALLOCX_ALIGN(kAlignment));
#else
    std::free(ptr);
#endif
  }
}

Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  uint8_t* previous_ptr = *ptr;
  if (previous_ptr == zero_size_area) {
    DCHECK_EQ(old_size, 0);
    return AllocateAligned(new_size, ptr);
  }
  if (new_size == 0) {
    DeallocateAligned(previous_ptr, old_size);
    *ptr = zero_size_area;
    return Status::OK();
  }
#ifdef ARROW_JEMALLOC
  if (new_size < 0) {
    return Status::Invalid("negative realloc size");
  }
  if (static_cast<uint64_t>(new_size) >= std::numeric_limits<size_t>::max()) {
    return Status::CapacityError("realloc overflows size_t");
  }
  *ptr = reinterpret_cast<uint8_t*>(
      rallocx(*ptr, static_cast<size_t>(new_size), MALLOCX_ALIGN(kAlignment)));
  if (*ptr == NULL) {
    *ptr = previous_ptr;
    return Status::OutOfMemory("realloc of size ", new_size, " failed");
  }
#else
  // Note: We cannot use realloc() here as it doesn't guarantee alignment.

  // Allocate new chunk
  uint8_t* out = nullptr;
  RETURN_NOT_OK(AllocateAligned(new_size, &out));
  DCHECK(out);
  // Copy contents and release old memory chunk
  memcpy(out, *ptr, static_cast<size_t>(std::min(new_size, old_size)));
#ifdef _WIN32
  _aligned_free(*ptr);
#else
  std::free(*ptr);
#endif  // defined(_MSC_VER)
  *ptr = out;
#endif  // defined(ARROW_JEMALLOC)

  return Status::OK();
}

}  // namespace

MemoryPool::MemoryPool() {}

MemoryPool::~MemoryPool() {}

int64_t MemoryPool::max_memory() const { return -1; }

///////////////////////////////////////////////////////////////////////
// Default MemoryPool implementation

class DefaultMemoryPool : public MemoryPool {
 public:
  ~DefaultMemoryPool() override {}

  Status Allocate(int64_t size, uint8_t** out) override {
    RETURN_NOT_OK(AllocateAligned(size, out));

    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    RETURN_NOT_OK(ReallocateAligned(old_size, new_size, ptr));

    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  int64_t bytes_allocated() const override { return stats_.bytes_allocated(); }

  void Free(uint8_t* buffer, int64_t size) override {
    DeallocateAligned(buffer, size);

    stats_.UpdateAllocatedBytes(-size);
  }

  int64_t max_memory() const override { return stats_.max_memory(); }

 private:
  internal::MemoryPoolStats stats_;
};

std::unique_ptr<MemoryPool> MemoryPool::CreateDefault() {
  return std::unique_ptr<MemoryPool>(new DefaultMemoryPool);
}

MemoryPool* default_memory_pool() {
  static DefaultMemoryPool default_memory_pool_;
  return &default_memory_pool_;
}

///////////////////////////////////////////////////////////////////////
// LoggingMemoryPool implementation

LoggingMemoryPool::LoggingMemoryPool(MemoryPool* pool) : pool_(pool) {}

Status LoggingMemoryPool::Allocate(int64_t size, uint8_t** out) {
  Status s = pool_->Allocate(size, out);
  std::cout << "Allocate: size = " << size << std::endl;
  return s;
}

Status LoggingMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  Status s = pool_->Reallocate(old_size, new_size, ptr);
  std::cout << "Reallocate: old_size = " << old_size << " - new_size = " << new_size
            << std::endl;
  return s;
}

void LoggingMemoryPool::Free(uint8_t* buffer, int64_t size) {
  pool_->Free(buffer, size);
  std::cout << "Free: size = " << size << std::endl;
}

int64_t LoggingMemoryPool::bytes_allocated() const {
  int64_t nb_bytes = pool_->bytes_allocated();
  std::cout << "bytes_allocated: " << nb_bytes << std::endl;
  return nb_bytes;
}

int64_t LoggingMemoryPool::max_memory() const {
  int64_t mem = pool_->max_memory();
  std::cout << "max_memory: " << mem << std::endl;
  return mem;
}

///////////////////////////////////////////////////////////////////////
// ProxyMemoryPool implementation

class ProxyMemoryPool::ProxyMemoryPoolImpl {
 public:
  explicit ProxyMemoryPoolImpl(MemoryPool* pool) : pool_(pool) {}

  Status Allocate(int64_t size, uint8_t** out) {
    RETURN_NOT_OK(pool_->Allocate(size, out));
    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    RETURN_NOT_OK(pool_->Reallocate(old_size, new_size, ptr));
    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) {
    pool_->Free(buffer, size);
    stats_.UpdateAllocatedBytes(-size);
  }

  int64_t bytes_allocated() const { return stats_.bytes_allocated(); }

  int64_t max_memory() const { return stats_.max_memory(); }

 private:
  MemoryPool* pool_;
  internal::MemoryPoolStats stats_;
};

ProxyMemoryPool::ProxyMemoryPool(MemoryPool* pool) {
  impl_.reset(new ProxyMemoryPoolImpl(pool));
}

ProxyMemoryPool::~ProxyMemoryPool() {}

Status ProxyMemoryPool::Allocate(int64_t size, uint8_t** out) {
  return impl_->Allocate(size, out);
}

Status ProxyMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  return impl_->Reallocate(old_size, new_size, ptr);
}

void ProxyMemoryPool::Free(uint8_t* buffer, int64_t size) {
  return impl_->Free(buffer, size);
}

int64_t ProxyMemoryPool::bytes_allocated() const { return impl_->bytes_allocated(); }

int64_t ProxyMemoryPool::max_memory() const { return impl_->max_memory(); }

}  // namespace arrow
