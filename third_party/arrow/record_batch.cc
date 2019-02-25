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

#include "arrow/record_batch.h"

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"

namespace arrow {

Status RecordBatch::AddColumn(int i, const std::string& field_name,
                              const std::shared_ptr<Array>& column,
                              std::shared_ptr<RecordBatch>* out) const {
  auto field = ::arrow::field(field_name, column->type());
  return AddColumn(i, field, column, out);
}

/// \class SimpleRecordBatch
/// \brief A basic, non-lazy in-memory record batch
class SimpleRecordBatch : public RecordBatch {
 public:
  SimpleRecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
                    const std::vector<std::shared_ptr<Array>>& columns)
      : RecordBatch(schema, num_rows) {
    columns_.resize(columns.size());
    boxed_columns_.resize(schema->num_fields());
    for (size_t i = 0; i < columns.size(); ++i) {
      columns_[i] = columns[i]->data();
    }
  }

  SimpleRecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
                    std::vector<std::shared_ptr<Array>>&& columns)
      : RecordBatch(schema, num_rows) {
    columns_.resize(columns.size());
    boxed_columns_.resize(schema->num_fields());
    for (size_t i = 0; i < columns.size(); ++i) {
      columns_[i] = columns[i]->data();
    }
  }

  SimpleRecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
                    std::vector<std::shared_ptr<ArrayData>>&& columns)
      : RecordBatch(schema, num_rows) {
    columns_ = std::move(columns);
    boxed_columns_.resize(schema->num_fields());
  }

  SimpleRecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
                    const std::vector<std::shared_ptr<ArrayData>>& columns)
      : RecordBatch(schema, num_rows) {
    columns_ = columns;
    boxed_columns_.resize(schema->num_fields());
  }

  std::shared_ptr<Array> column(int i) const override {
    if (!boxed_columns_[i]) {
      boxed_columns_[i] = MakeArray(columns_[i]);
    }
    DCHECK(boxed_columns_[i]);
    return boxed_columns_[i];
  }

  std::shared_ptr<ArrayData> column_data(int i) const override { return columns_[i]; }

  Status AddColumn(int i, const std::shared_ptr<Field>& field,
                   const std::shared_ptr<Array>& column,
                   std::shared_ptr<RecordBatch>* out) const override {
    DCHECK(field != nullptr);
    DCHECK(column != nullptr);

    if (!field->type()->Equals(column->type())) {
      return Status::Invalid("Column data type ", field->type()->name(),
                             " does not match field data type ", column->type()->name());
    }
    if (column->length() != num_rows_) {
      return Status::Invalid(
          "Added column's length must match record batch's length. Expected length ",
          num_rows_, " but got length ", column->length());
    }

    std::shared_ptr<Schema> new_schema;
    RETURN_NOT_OK(schema_->AddField(i, field, &new_schema));

    *out = RecordBatch::Make(new_schema, num_rows_,
                             internal::AddVectorElement(columns_, i, column->data()));
    return Status::OK();
  }

  Status RemoveColumn(int i, std::shared_ptr<RecordBatch>* out) const override {
    std::shared_ptr<Schema> new_schema;
    RETURN_NOT_OK(schema_->RemoveField(i, &new_schema));

    *out = RecordBatch::Make(new_schema, num_rows_,
                             internal::DeleteVectorElement(columns_, i));
    return Status::OK();
  }

  std::shared_ptr<RecordBatch> ReplaceSchemaMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const override {
    auto new_schema = schema_->AddMetadata(metadata);
    return RecordBatch::Make(new_schema, num_rows_, columns_);
  }

  std::shared_ptr<RecordBatch> Slice(int64_t offset, int64_t length) const override {
    std::vector<std::shared_ptr<ArrayData>> arrays;
    arrays.reserve(num_columns());
    for (const auto& field : columns_) {
      int64_t col_length = std::min(field->length - offset, length);
      int64_t col_offset = field->offset + offset;

      auto new_data = std::make_shared<ArrayData>(*field);
      new_data->length = col_length;
      new_data->offset = col_offset;
      new_data->null_count = kUnknownNullCount;
      arrays.emplace_back(new_data);
    }
    int64_t num_rows = std::min(num_rows_ - offset, length);
    return std::make_shared<SimpleRecordBatch>(schema_, num_rows, std::move(arrays));
  }

  Status Validate() const override {
    if (static_cast<int>(columns_.size()) != schema_->num_fields()) {
      return Status::Invalid("Number of columns did not match schema");
    }
    return RecordBatch::Validate();
  }

 private:
  std::vector<std::shared_ptr<ArrayData>> columns_;

  // Caching boxed array data
  mutable std::vector<std::shared_ptr<Array>> boxed_columns_;
};

RecordBatch::RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows)
    : schema_(schema), num_rows_(num_rows) {}

std::shared_ptr<RecordBatch> RecordBatch::Make(
    const std::shared_ptr<Schema>& schema, int64_t num_rows,
    const std::vector<std::shared_ptr<Array>>& columns) {
  return std::make_shared<SimpleRecordBatch>(schema, num_rows, columns);
}

std::shared_ptr<RecordBatch> RecordBatch::Make(
    const std::shared_ptr<Schema>& schema, int64_t num_rows,
    std::vector<std::shared_ptr<Array>>&& columns) {
  return std::make_shared<SimpleRecordBatch>(schema, num_rows, std::move(columns));
}

std::shared_ptr<RecordBatch> RecordBatch::Make(
    const std::shared_ptr<Schema>& schema, int64_t num_rows,
    std::vector<std::shared_ptr<ArrayData>>&& columns) {
  return std::make_shared<SimpleRecordBatch>(schema, num_rows, std::move(columns));
}

std::shared_ptr<RecordBatch> RecordBatch::Make(
    const std::shared_ptr<Schema>& schema, int64_t num_rows,
    const std::vector<std::shared_ptr<ArrayData>>& columns) {
  return std::make_shared<SimpleRecordBatch>(schema, num_rows, columns);
}

const std::string& RecordBatch::column_name(int i) const {
  return schema_->field(i)->name();
}

bool RecordBatch::Equals(const RecordBatch& other) const {
  if (num_columns() != other.num_columns() || num_rows_ != other.num_rows()) {
    return false;
  }

  for (int i = 0; i < num_columns(); ++i) {
    if (!column(i)->Equals(other.column(i))) {
      return false;
    }
  }

  return true;
}

bool RecordBatch::ApproxEquals(const RecordBatch& other) const {
  if (num_columns() != other.num_columns() || num_rows_ != other.num_rows()) {
    return false;
  }

  for (int i = 0; i < num_columns(); ++i) {
    if (!column(i)->ApproxEquals(other.column(i))) {
      return false;
    }
  }

  return true;
}

std::shared_ptr<RecordBatch> RecordBatch::Slice(int64_t offset) const {
  return Slice(offset, this->num_rows() - offset);
}

Status RecordBatch::Validate() const {
  for (int i = 0; i < num_columns(); ++i) {
    auto arr_shared = this->column_data(i);
    const ArrayData& arr = *arr_shared;
    if (arr.length != num_rows_) {
      return Status::Invalid("Number of rows in column ", i,
                             " did not match batch: ", arr.length, " vs ", num_rows_);
    }
    const auto& schema_type = *schema_->field(i)->type();
    if (!arr.type->Equals(schema_type)) {
      return Status::Invalid("Column ", i,
                             " type not match schema: ", arr.type->ToString(), " vs ",
                             schema_type.ToString());
    }
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Base record batch reader

RecordBatchReader::~RecordBatchReader() {}

Status RecordBatchReader::ReadAll(std::vector<std::shared_ptr<RecordBatch>>* batches) {
  while (true) {
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(ReadNext(&batch));
    if (!batch) {
      break;
    }
    batches->emplace_back(std::move(batch));
  }
  return Status::OK();
}

Status RecordBatchReader::ReadAll(std::shared_ptr<Table>* table) {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  RETURN_NOT_OK(ReadAll(&batches));
  return Table::FromRecordBatches(schema(), batches, table);
}

}  // namespace arrow
