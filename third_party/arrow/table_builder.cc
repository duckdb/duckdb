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

#include "arrow/table_builder.h"

#include <memory>
#include <utility>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {

// ----------------------------------------------------------------------
// RecordBatchBuilder

RecordBatchBuilder::RecordBatchBuilder(const std::shared_ptr<Schema>& schema,
                                       MemoryPool* pool, int64_t initial_capacity)
    : schema_(schema), initial_capacity_(initial_capacity), pool_(pool) {}

Status RecordBatchBuilder::Make(const std::shared_ptr<Schema>& schema, MemoryPool* pool,
                                std::unique_ptr<RecordBatchBuilder>* builder) {
  return Make(schema, pool, kMinBuilderCapacity, builder);
}

Status RecordBatchBuilder::Make(const std::shared_ptr<Schema>& schema, MemoryPool* pool,
                                int64_t initial_capacity,
                                std::unique_ptr<RecordBatchBuilder>* builder) {
  builder->reset(new RecordBatchBuilder(schema, pool, initial_capacity));
  RETURN_NOT_OK((*builder)->CreateBuilders());
  return (*builder)->InitBuilders();
}

Status RecordBatchBuilder::Flush(bool reset_builders,
                                 std::shared_ptr<RecordBatch>* batch) {
  std::vector<std::shared_ptr<Array>> fields;
  fields.resize(this->num_fields());

  int64_t length = 0;
  for (int i = 0; i < this->num_fields(); ++i) {
    RETURN_NOT_OK(raw_field_builders_[i]->Finish(&fields[i]));
    if (i > 0 && fields[i]->length() != length) {
      return Status::Invalid("All fields must be same length when calling Flush");
    }
    length = fields[i]->length();
  }
  *batch = RecordBatch::Make(schema_, length, std::move(fields));
  if (reset_builders) {
    return InitBuilders();
  } else {
    return Status::OK();
  }
}

Status RecordBatchBuilder::Flush(std::shared_ptr<RecordBatch>* batch) {
  return Flush(true, batch);
}

void RecordBatchBuilder::SetInitialCapacity(int64_t capacity) {
  DCHECK_GT(capacity, 0) << "Initial capacity must be positive";
  initial_capacity_ = capacity;
}

Status RecordBatchBuilder::CreateBuilders() {
  field_builders_.resize(this->num_fields());
  raw_field_builders_.resize(this->num_fields());
  for (int i = 0; i < this->num_fields(); ++i) {
    RETURN_NOT_OK(MakeBuilder(pool_, schema_->field(i)->type(), &field_builders_[i]));
    raw_field_builders_[i] = field_builders_[i].get();
  }
  return Status::OK();
}

Status RecordBatchBuilder::InitBuilders() {
  for (int i = 0; i < this->num_fields(); ++i) {
    RETURN_NOT_OK(raw_field_builders_[i]->Reserve(initial_capacity_));
  }
  return Status::OK();
}

}  // namespace arrow
