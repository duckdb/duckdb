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

#include "arrow/array/builder_adaptive.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/int-util.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::AdaptiveIntBuilderBase;

AdaptiveIntBuilderBase::AdaptiveIntBuilderBase(MemoryPool* pool)
    : ArrayBuilder(int64(), pool),
      data_(nullptr),
      raw_data_(nullptr),
      int_size_(1),
      pending_pos_(0),
      pending_has_nulls_(false) {}

void AdaptiveIntBuilderBase::Reset() {
  ArrayBuilder::Reset();
  data_.reset();
  raw_data_ = nullptr;
  pending_pos_ = 0;
  pending_has_nulls_ = false;
}

Status AdaptiveIntBuilderBase::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));
  capacity = std::max(capacity, kMinBuilderCapacity);

  int64_t nbytes = capacity * int_size_;
  if (capacity_ == 0) {
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, nbytes, &data_));
  } else {
    RETURN_NOT_OK(data_->Resize(nbytes));
  }
  raw_data_ = reinterpret_cast<uint8_t*>(data_->mutable_data());

  return ArrayBuilder::Resize(capacity);
}

AdaptiveIntBuilder::AdaptiveIntBuilder(MemoryPool* pool) : AdaptiveIntBuilderBase(pool) {}

Status AdaptiveIntBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  RETURN_NOT_OK(CommitPendingData());

  std::shared_ptr<DataType> output_type;
  switch (int_size_) {
    case 1:
      output_type = int8();
      break;
    case 2:
      output_type = int16();
      break;
    case 4:
      output_type = int32();
      break;
    case 8:
      output_type = int64();
      break;
    default:
      DCHECK(false);
      return Status::NotImplemented("Only ints of size 1,2,4,8 are supported");
  }

  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
  RETURN_NOT_OK(TrimBuffer(length_ * int_size_, data_.get()));

  *out = ArrayData::Make(output_type, length_, {null_bitmap, data_}, null_count_);

  data_ = nullptr;
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

Status AdaptiveIntBuilder::CommitPendingData() {
  if (pending_pos_ == 0) {
    return Status::OK();
  }
  RETURN_NOT_OK(Reserve(pending_pos_));
  const uint8_t* valid_bytes = pending_has_nulls_ ? pending_valid_ : nullptr;
  RETURN_NOT_OK(AppendValuesInternal(reinterpret_cast<const int64_t*>(pending_data_),
                                     pending_pos_, valid_bytes));
  pending_has_nulls_ = false;
  pending_pos_ = 0;
  return Status::OK();
}

static constexpr int64_t kAdaptiveIntChunkSize = 8192;

Status AdaptiveIntBuilder::AppendValuesInternal(const int64_t* values, int64_t length,
                                                const uint8_t* valid_bytes) {
  while (length > 0) {
    // In case `length` is very large, we don't want to trash the cache by
    // scanning it twice (first to detect int width, second to copy the data).
    // Instead, process data in L2-cacheable chunks.
    const int64_t chunk_size = std::min(length, kAdaptiveIntChunkSize);

    uint8_t new_int_size;
    new_int_size = internal::DetectIntWidth(values, valid_bytes, chunk_size, int_size_);

    DCHECK_GE(new_int_size, int_size_);
    if (new_int_size > int_size_) {
      // This updates int_size_
      RETURN_NOT_OK(ExpandIntSize(new_int_size));
    }

    switch (int_size_) {
      case 1:
        internal::DowncastInts(values, reinterpret_cast<int8_t*>(raw_data_) + length_,
                               chunk_size);
        break;
      case 2:
        internal::DowncastInts(values, reinterpret_cast<int16_t*>(raw_data_) + length_,
                               chunk_size);
        break;
      case 4:
        internal::DowncastInts(values, reinterpret_cast<int32_t*>(raw_data_) + length_,
                               chunk_size);
        break;
      case 8:
        internal::DowncastInts(values, reinterpret_cast<int64_t*>(raw_data_) + length_,
                               chunk_size);
        break;
      default:
        DCHECK(false);
    }

    // This updates length_
    ArrayBuilder::UnsafeAppendToBitmap(valid_bytes, chunk_size);
    values += chunk_size;
    if (valid_bytes != nullptr) {
      valid_bytes += chunk_size;
    }
    length -= chunk_size;
  }

  return Status::OK();
}

Status AdaptiveUIntBuilder::CommitPendingData() {
  if (pending_pos_ == 0) {
    return Status::OK();
  }
  RETURN_NOT_OK(Reserve(pending_pos_));
  const uint8_t* valid_bytes = pending_has_nulls_ ? pending_valid_ : nullptr;
  RETURN_NOT_OK(AppendValuesInternal(pending_data_, pending_pos_, valid_bytes));
  pending_has_nulls_ = false;
  pending_pos_ = 0;
  return Status::OK();
}

Status AdaptiveIntBuilder::AppendValues(const int64_t* values, int64_t length,
                                        const uint8_t* valid_bytes) {
  RETURN_NOT_OK(CommitPendingData());
  RETURN_NOT_OK(Reserve(length));

  return AppendValuesInternal(values, length, valid_bytes);
}

template <typename new_type, typename old_type>
typename std::enable_if<sizeof(old_type) >= sizeof(new_type), Status>::type
AdaptiveIntBuilder::ExpandIntSizeInternal() {
  return Status::OK();
}

#define __LESS(a, b) (a) < (b)
template <typename new_type, typename old_type>
typename std::enable_if<__LESS(sizeof(old_type), sizeof(new_type)), Status>::type
AdaptiveIntBuilder::ExpandIntSizeInternal() {
  int_size_ = sizeof(new_type);
  RETURN_NOT_OK(Resize(data_->size() / sizeof(old_type)));
  raw_data_ = reinterpret_cast<uint8_t*>(data_->mutable_data());
  const old_type* src = reinterpret_cast<old_type*>(raw_data_);
  new_type* dst = reinterpret_cast<new_type*>(raw_data_);

  // By doing the backward copy, we ensure that no element is overriden during
  // the copy process and the copy stays in-place.
  std::copy_backward(src, src + length_, dst + length_);

  return Status::OK();
}
#undef __LESS

template <typename new_type>
Status AdaptiveIntBuilder::ExpandIntSizeN() {
  switch (int_size_) {
    case 1:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, int8_t>()));
      break;
    case 2:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, int16_t>()));
      break;
    case 4:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, int32_t>()));
      break;
    case 8:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, int64_t>()));
      break;
    default:
      DCHECK(false);
  }
  return Status::OK();
}

Status AdaptiveIntBuilder::ExpandIntSize(uint8_t new_int_size) {
  switch (new_int_size) {
    case 1:
      RETURN_NOT_OK((ExpandIntSizeN<int8_t>()));
      break;
    case 2:
      RETURN_NOT_OK((ExpandIntSizeN<int16_t>()));
      break;
    case 4:
      RETURN_NOT_OK((ExpandIntSizeN<int32_t>()));
      break;
    case 8:
      RETURN_NOT_OK((ExpandIntSizeN<int64_t>()));
      break;
    default:
      DCHECK(false);
  }
  return Status::OK();
}

AdaptiveUIntBuilder::AdaptiveUIntBuilder(MemoryPool* pool)
    : AdaptiveIntBuilderBase(pool) {}

Status AdaptiveUIntBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  RETURN_NOT_OK(CommitPendingData());

  std::shared_ptr<DataType> output_type;
  switch (int_size_) {
    case 1:
      output_type = uint8();
      break;
    case 2:
      output_type = uint16();
      break;
    case 4:
      output_type = uint32();
      break;
    case 8:
      output_type = uint64();
      break;
    default:
      DCHECK(false);
      return Status::NotImplemented("Only ints of size 1,2,4,8 are supported");
  }

  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
  RETURN_NOT_OK(TrimBuffer(length_ * int_size_, data_.get()));

  *out = ArrayData::Make(output_type, length_, {null_bitmap, data_}, null_count_);

  data_ = nullptr;
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

Status AdaptiveUIntBuilder::AppendValuesInternal(const uint64_t* values, int64_t length,
                                                 const uint8_t* valid_bytes) {
  while (length > 0) {
    // See AdaptiveIntBuilder::AppendValuesInternal
    const int64_t chunk_size = std::min(length, kAdaptiveIntChunkSize);

    uint8_t new_int_size;
    new_int_size = internal::DetectUIntWidth(values, valid_bytes, chunk_size, int_size_);

    DCHECK_GE(new_int_size, int_size_);
    if (new_int_size > int_size_) {
      // This updates int_size_
      RETURN_NOT_OK(ExpandIntSize(new_int_size));
    }

    switch (int_size_) {
      case 1:
        internal::DowncastUInts(values, reinterpret_cast<uint8_t*>(raw_data_) + length_,
                                chunk_size);
        break;
      case 2:
        internal::DowncastUInts(values, reinterpret_cast<uint16_t*>(raw_data_) + length_,
                                chunk_size);
        break;
      case 4:
        internal::DowncastUInts(values, reinterpret_cast<uint32_t*>(raw_data_) + length_,
                                chunk_size);
        break;
      case 8:
        internal::DowncastUInts(values, reinterpret_cast<uint64_t*>(raw_data_) + length_,
                                chunk_size);
        break;
      default:
        DCHECK(false);
    }

    // This updates length_
    ArrayBuilder::UnsafeAppendToBitmap(valid_bytes, chunk_size);
    values += chunk_size;
    if (valid_bytes != nullptr) {
      valid_bytes += chunk_size;
    }
    length -= chunk_size;
  }

  return Status::OK();
}

Status AdaptiveUIntBuilder::AppendValues(const uint64_t* values, int64_t length,
                                         const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));

  return AppendValuesInternal(values, length, valid_bytes);
}

template <typename new_type, typename old_type>
typename std::enable_if<sizeof(old_type) >= sizeof(new_type), Status>::type
AdaptiveUIntBuilder::ExpandIntSizeInternal() {
  return Status::OK();
}

#define __LESS(a, b) (a) < (b)
template <typename new_type, typename old_type>
typename std::enable_if<__LESS(sizeof(old_type), sizeof(new_type)), Status>::type
AdaptiveUIntBuilder::ExpandIntSizeInternal() {
  int_size_ = sizeof(new_type);
  RETURN_NOT_OK(Resize(data_->size() / sizeof(old_type)));

  old_type* src = reinterpret_cast<old_type*>(raw_data_);
  new_type* dst = reinterpret_cast<new_type*>(raw_data_);
  // By doing the backward copy, we ensure that no element is overriden during
  // the copy process and the copy stays in-place.
  std::copy_backward(src, src + length_, dst + length_);

  return Status::OK();
}
#undef __LESS

template <typename new_type>
Status AdaptiveUIntBuilder::ExpandIntSizeN() {
  switch (int_size_) {
    case 1:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, uint8_t>()));
      break;
    case 2:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, uint16_t>()));
      break;
    case 4:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, uint32_t>()));
      break;
    case 8:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, uint64_t>()));
      break;
    default:
      DCHECK(false);
  }
  return Status::OK();
}

Status AdaptiveUIntBuilder::ExpandIntSize(uint8_t new_int_size) {
  switch (new_int_size) {
    case 1:
      RETURN_NOT_OK((ExpandIntSizeN<uint8_t>()));
      break;
    case 2:
      RETURN_NOT_OK((ExpandIntSizeN<uint16_t>()));
      break;
    case 4:
      RETURN_NOT_OK((ExpandIntSizeN<uint32_t>()));
      break;
    case 8:
      RETURN_NOT_OK((ExpandIntSizeN<uint64_t>()));
      break;
    default:
      DCHECK(false);
  }
  return Status::OK();
}

}  // namespace arrow
