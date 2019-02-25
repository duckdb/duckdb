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

#include "arrow/array/builder_nested.h"

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
// ListBuilder

ListBuilder::ListBuilder(MemoryPool* pool,
                         std::shared_ptr<ArrayBuilder> const& value_builder,
                         const std::shared_ptr<DataType>& type)
    : ArrayBuilder(type ? type
                        : std::static_pointer_cast<DataType>(
                              std::make_shared<ListType>(value_builder->type())),
                   pool),
      offsets_builder_(pool),
      value_builder_(value_builder) {}

Status ListBuilder::AppendValues(const int32_t* offsets, int64_t length,
                                 const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(valid_bytes, length);
  offsets_builder_.UnsafeAppend(offsets, length);
  return Status::OK();
}

Status ListBuilder::AppendNextOffset() {
  const int64_t num_values = value_builder_->length();
  ARROW_RETURN_IF(
      num_values > kListMaximumElements,
      Status::CapacityError("ListArray cannot contain more then 2^31 - 1 child elements,",
                            " have ", num_values));
  return offsets_builder_.Append(static_cast<int32_t>(num_values));
}

Status ListBuilder::Append(bool is_valid) {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(is_valid);
  return AppendNextOffset();
}

Status ListBuilder::Resize(int64_t capacity) {
  DCHECK_LE(capacity, kListMaximumElements);
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));

  // one more then requested for offsets
  RETURN_NOT_OK(offsets_builder_.Resize(capacity + 1));
  return ArrayBuilder::Resize(capacity);
}

Status ListBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  RETURN_NOT_OK(AppendNextOffset());

  // Offset padding zeroed by BufferBuilder
  std::shared_ptr<Buffer> offsets;
  RETURN_NOT_OK(offsets_builder_.Finish(&offsets));

  std::shared_ptr<ArrayData> items;
  if (values_) {
    items = values_->data();
  } else {
    if (value_builder_->length() == 0) {
      // Try to make sure we get a non-null values buffer (ARROW-2744)
      RETURN_NOT_OK(value_builder_->Resize(0));
    }
    RETURN_NOT_OK(value_builder_->FinishInternal(&items));
  }

  // If the type has not been specified in the constructor, infer it
  // This is the case if the value_builder contains a DenseUnionBuilder
  if (!arrow::internal::checked_cast<ListType&>(*type_).value_type()) {
    type_ = std::static_pointer_cast<DataType>(
        std::make_shared<ListType>(value_builder_->type()));
  }
  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
  *out = ArrayData::Make(type_, length_, {null_bitmap, offsets}, null_count_);
  (*out)->child_data.emplace_back(std::move(items));
  Reset();
  return Status::OK();
}

void ListBuilder::Reset() {
  ArrayBuilder::Reset();
  values_.reset();
  offsets_builder_.Reset();
  value_builder_->Reset();
}

ArrayBuilder* ListBuilder::value_builder() const {
  DCHECK(!values_) << "Using value builder is pointless when values_ is set";
  return value_builder_.get();
}

// ----------------------------------------------------------------------
// Struct

StructBuilder::StructBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool,
                             std::vector<std::shared_ptr<ArrayBuilder>>&& field_builders)
    : ArrayBuilder(type, pool) {
  children_ = std::move(field_builders);
}

void StructBuilder::Reset() {
  ArrayBuilder::Reset();
  for (const auto& field_builder : children_) {
    field_builder->Reset();
  }
}

Status StructBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

  std::vector<std::shared_ptr<ArrayData>> child_data(children_.size());
  for (size_t i = 0; i < children_.size(); ++i) {
    if (length_ == 0) {
      // Try to make sure the child buffers are initialized
      RETURN_NOT_OK(children_[i]->Resize(0));
    }
    RETURN_NOT_OK(children_[i]->FinishInternal(&child_data[i]));
  }

  // If the type has not been specified in the constructor, infer it
  // This is the case if one of the children contains a DenseUnionBuilder
  if (!type_) {
    std::vector<std::shared_ptr<Field>> fields;
    for (const auto& field_builder : children_) {
      fields.push_back(field("", field_builder->type()));
    }
    type_ = struct_(fields);
  }

  *out = ArrayData::Make(type_, length_, {null_bitmap}, null_count_);
  (*out)->child_data = std::move(child_data);

  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

}  // namespace arrow
