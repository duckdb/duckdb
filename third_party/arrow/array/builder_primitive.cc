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

#include "arrow/array/builder_primitive.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/int-util.h"
#include "arrow/util/logging.h"

namespace arrow {

// ----------------------------------------------------------------------
// Null builder

Status NullBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  *out = ArrayData::Make(null(), length_, {nullptr}, length_);
  length_ = null_count_ = 0;
  return Status::OK();
}

BooleanBuilder::BooleanBuilder(MemoryPool* pool)
    : ArrayBuilder(boolean(), pool), data_builder_(pool) {}

BooleanBuilder::BooleanBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool)
    : BooleanBuilder(pool) {
  DCHECK_EQ(Type::BOOL, type->id());
}

void BooleanBuilder::Reset() {
  ArrayBuilder::Reset();
  data_builder_.Reset();
}

Status BooleanBuilder::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));
  capacity = std::max(capacity, kMinBuilderCapacity);
  RETURN_NOT_OK(data_builder_.Resize(capacity));
  return ArrayBuilder::Resize(capacity);
}

Status BooleanBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> data, null_bitmap;
  RETURN_NOT_OK(data_builder_.Finish(&data));
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

  *out = ArrayData::Make(boolean(), length_, {null_bitmap, data}, null_count_);

  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

Status BooleanBuilder::AppendValues(const uint8_t* values, int64_t length,
                                    const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));

  int64_t i = 0;
  data_builder_.UnsafeAppend<false>(length,
                                    [values, &i]() -> bool { return values[i++] != 0; });
  ArrayBuilder::UnsafeAppendToBitmap(valid_bytes, length);
  return Status::OK();
}

Status BooleanBuilder::AppendValues(const uint8_t* values, int64_t length,
                                    const std::vector<bool>& is_valid) {
  RETURN_NOT_OK(Reserve(length));
  DCHECK_EQ(length, static_cast<int64_t>(is_valid.size()));
  int64_t i = 0;
  data_builder_.UnsafeAppend<false>(length,
                                    [values, &i]() -> bool { return values[i++]; });
  ArrayBuilder::UnsafeAppendToBitmap(is_valid);
  return Status::OK();
}

Status BooleanBuilder::AppendValues(const std::vector<uint8_t>& values,
                                    const std::vector<bool>& is_valid) {
  return AppendValues(values.data(), static_cast<int64_t>(values.size()), is_valid);
}

Status BooleanBuilder::AppendValues(const std::vector<uint8_t>& values) {
  return AppendValues(values.data(), static_cast<int64_t>(values.size()));
}

Status BooleanBuilder::AppendValues(const std::vector<bool>& values,
                                    const std::vector<bool>& is_valid) {
  const int64_t length = static_cast<int64_t>(values.size());
  RETURN_NOT_OK(Reserve(length));
  DCHECK_EQ(length, static_cast<int64_t>(is_valid.size()));
  int64_t i = 0;
  data_builder_.UnsafeAppend<false>(length,
                                    [&values, &i]() -> bool { return values[i++]; });
  ArrayBuilder::UnsafeAppendToBitmap(is_valid);
  return Status::OK();
}

Status BooleanBuilder::AppendValues(const std::vector<bool>& values) {
  const int64_t length = static_cast<int64_t>(values.size());
  RETURN_NOT_OK(Reserve(length));
  int64_t i = 0;
  data_builder_.UnsafeAppend<false>(length,
                                    [&values, &i]() -> bool { return values[i++]; });
  ArrayBuilder::UnsafeSetNotNull(length);
  return Status::OK();
}

}  // namespace arrow
