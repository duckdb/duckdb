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
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

using std::shared_ptr;
using std::vector;

namespace arrow {

std::shared_ptr<Column> column(const std::shared_ptr<Field>& field,
                               const std::vector<std::shared_ptr<Array>>& arrays) {
  return std::make_shared<Column>(field, arrays);
}

class TestChunkedArray : public TestBase {
 protected:
  virtual void Construct() {
    one_ = std::make_shared<ChunkedArray>(arrays_one_);
    if (!arrays_another_.empty()) {
      another_ = std::make_shared<ChunkedArray>(arrays_another_);
    }
  }

  ArrayVector arrays_one_;
  ArrayVector arrays_another_;

  std::shared_ptr<ChunkedArray> one_;
  std::shared_ptr<ChunkedArray> another_;
};

TEST_F(TestChunkedArray, BasicEquals) {
  std::vector<bool> null_bitmap(100, true);
  std::vector<int32_t> data(100, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap, data, &array);
  arrays_one_.push_back(array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_TRUE(one_->Equals(one_));
  ASSERT_FALSE(one_->Equals(nullptr));
  ASSERT_TRUE(one_->Equals(another_));
  ASSERT_TRUE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, EqualsDifferingTypes) {
  std::vector<bool> null_bitmap(100, true);
  std::vector<int32_t> data32(100, 1);
  std::vector<int64_t> data64(100, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap, data32, &array);
  arrays_one_.push_back(array);
  ArrayFromVector<Int64Type, int64_t>(null_bitmap, data64, &array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_FALSE(one_->Equals(another_));
  ASSERT_FALSE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, EqualsDifferingLengths) {
  std::vector<bool> null_bitmap100(100, true);
  std::vector<bool> null_bitmap101(101, true);
  std::vector<int32_t> data100(100, 1);
  std::vector<int32_t> data101(101, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap100, data100, &array);
  arrays_one_.push_back(array);
  ArrayFromVector<Int32Type, int32_t>(null_bitmap101, data101, &array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_FALSE(one_->Equals(another_));
  ASSERT_FALSE(one_->Equals(*another_.get()));

  std::vector<bool> null_bitmap1(1, true);
  std::vector<int32_t> data1(1, 1);
  ArrayFromVector<Int32Type, int32_t>(null_bitmap1, data1, &array);
  arrays_one_.push_back(array);

  Construct();
  ASSERT_TRUE(one_->Equals(another_));
  ASSERT_TRUE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, SliceEquals) {
  arrays_one_.push_back(MakeRandomArray<Int32Array>(100));
  arrays_one_.push_back(MakeRandomArray<Int32Array>(50));
  arrays_one_.push_back(MakeRandomArray<Int32Array>(50));
  Construct();

  std::shared_ptr<ChunkedArray> slice = one_->Slice(125, 50);
  ASSERT_EQ(slice->length(), 50);
  AssertChunkedEqual(*one_->Slice(125, 50), *slice);

  std::shared_ptr<ChunkedArray> slice2 = one_->Slice(75)->Slice(25)->Slice(25, 50);
  ASSERT_EQ(slice2->length(), 50);
  AssertChunkedEqual(*slice, *slice2);

  // Making empty slices of a ChunkedArray
  std::shared_ptr<ChunkedArray> slice3 = one_->Slice(one_->length(), 99);
  ASSERT_EQ(slice3->length(), 0);
  ASSERT_EQ(slice3->num_chunks(), 0);
  ASSERT_TRUE(slice3->type()->Equals(one_->type()));

  std::shared_ptr<ChunkedArray> slice4 = one_->Slice(10, 0);
  ASSERT_EQ(slice4->length(), 0);
  ASSERT_EQ(slice4->num_chunks(), 0);
  ASSERT_TRUE(slice4->type()->Equals(one_->type()));

  // Slicing an empty ChunkedArray
  std::shared_ptr<ChunkedArray> slice5 = slice4->Slice(0, 10);
  ASSERT_EQ(slice5->length(), 0);
  ASSERT_EQ(slice5->num_chunks(), 0);
  ASSERT_TRUE(slice5->type()->Equals(one_->type()));
}

class TestColumn : public TestChunkedArray {
 protected:
  void Construct() override {
    TestChunkedArray::Construct();

    one_col_ = std::make_shared<Column>(one_field_, one_);
    another_col_ = std::make_shared<Column>(another_field_, another_);
  }

  std::shared_ptr<ChunkedArray> data_;
  std::unique_ptr<Column> column_;

  std::shared_ptr<Field> one_field_;
  std::shared_ptr<Field> another_field_;

  std::shared_ptr<Column> one_col_;
  std::shared_ptr<Column> another_col_;
};

TEST_F(TestColumn, BasicAPI) {
  ArrayVector arrays;
  arrays.push_back(MakeRandomArray<Int32Array>(100));
  arrays.push_back(MakeRandomArray<Int32Array>(100, 10));
  arrays.push_back(MakeRandomArray<Int32Array>(100, 20));

  auto f0 = field("c0", int32());
  column_.reset(new Column(f0, arrays));

  ASSERT_EQ("c0", column_->name());
  ASSERT_TRUE(column_->type()->Equals(int32()));
  ASSERT_EQ(300, column_->length());
  ASSERT_EQ(30, column_->null_count());
  ASSERT_EQ(3, column_->data()->num_chunks());
}

TEST_F(TestColumn, ChunksInhomogeneous) {
  ArrayVector arrays;
  arrays.push_back(MakeRandomArray<Int32Array>(100));
  arrays.push_back(MakeRandomArray<Int32Array>(100, 10));

  auto f0 = field("c0", int32());
  column_.reset(new Column(f0, arrays));

  ASSERT_OK(column_->ValidateData());

  arrays.push_back(MakeRandomArray<Int16Array>(100, 10));
  column_.reset(new Column(f0, arrays));
  ASSERT_RAISES(Invalid, column_->ValidateData());
}

TEST_F(TestColumn, SliceEquals) {
  arrays_one_.push_back(MakeRandomArray<Int32Array>(100));
  arrays_one_.push_back(MakeRandomArray<Int32Array>(50));
  arrays_one_.push_back(MakeRandomArray<Int32Array>(50));
  one_field_ = field("column", int32());
  Construct();

  std::shared_ptr<Column> slice = one_col_->Slice(125, 50);
  ASSERT_EQ(slice->length(), 50);
  ASSERT_TRUE(slice->Equals(one_col_->Slice(125, 50)));

  std::shared_ptr<Column> slice2 = one_col_->Slice(75)->Slice(25)->Slice(25, 50);
  ASSERT_EQ(slice2->length(), 50);
  ASSERT_TRUE(slice2->Equals(slice));
}

TEST_F(TestColumn, Equals) {
  std::vector<bool> null_bitmap(100, true);
  std::vector<int32_t> data(100, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap, data, &array);
  arrays_one_.push_back(array);
  arrays_another_.push_back(array);

  one_field_ = field("column", int32());
  another_field_ = field("column", int32());

  Construct();
  ASSERT_TRUE(one_col_->Equals(one_col_));
  ASSERT_FALSE(one_col_->Equals(nullptr));
  ASSERT_TRUE(one_col_->Equals(another_col_));
  ASSERT_TRUE(one_col_->Equals(*another_col_.get()));

  // Field is different
  another_field_ = field("two", int32());
  Construct();
  ASSERT_FALSE(one_col_->Equals(another_col_));
  ASSERT_FALSE(one_col_->Equals(*another_col_.get()));

  // ChunkedArray is different
  another_field_ = field("column", int32());
  arrays_another_.push_back(array);
  Construct();
  ASSERT_FALSE(one_col_->Equals(another_col_));
  ASSERT_FALSE(one_col_->Equals(*another_col_.get()));
}

class TestTable : public TestBase {
 public:
  void MakeExample1(int length) {
    auto f0 = field("f0", int32());
    auto f1 = field("f1", uint8());
    auto f2 = field("f2", int16());

    vector<shared_ptr<Field>> fields = {f0, f1, f2};
    schema_ = std::make_shared<Schema>(fields);

    arrays_ = {MakeRandomArray<Int32Array>(length), MakeRandomArray<UInt8Array>(length),
               MakeRandomArray<Int16Array>(length)};

    columns_ = {std::make_shared<Column>(schema_->field(0), arrays_[0]),
                std::make_shared<Column>(schema_->field(1), arrays_[1]),
                std::make_shared<Column>(schema_->field(2), arrays_[2])};
  }

 protected:
  std::shared_ptr<Table> table_;
  shared_ptr<Schema> schema_;

  std::vector<std::shared_ptr<Array>> arrays_;
  std::vector<std::shared_ptr<Column>> columns_;
};

TEST_F(TestTable, EmptySchema) {
  auto empty_schema = ::arrow::schema({});
  table_ = Table::Make(empty_schema, columns_);
  ASSERT_OK(table_->Validate());
  ASSERT_EQ(0, table_->num_rows());
  ASSERT_EQ(0, table_->num_columns());
}

TEST_F(TestTable, Ctors) {
  const int length = 100;
  MakeExample1(length);

  table_ = Table::Make(schema_, columns_);
  ASSERT_OK(table_->Validate());
  ASSERT_EQ(length, table_->num_rows());
  ASSERT_EQ(3, table_->num_columns());

  auto array_ctor = Table::Make(schema_, arrays_);
  ASSERT_TRUE(table_->Equals(*array_ctor));

  table_ = Table::Make(schema_, columns_, length);
  ASSERT_OK(table_->Validate());
  ASSERT_EQ(length, table_->num_rows());

  table_ = Table::Make(schema_, arrays_);
  ASSERT_OK(table_->Validate());
  ASSERT_EQ(length, table_->num_rows());
  ASSERT_EQ(3, table_->num_columns());
}

TEST_F(TestTable, Metadata) {
  const int length = 100;
  MakeExample1(length);

  table_ = Table::Make(schema_, columns_);

  ASSERT_TRUE(table_->schema()->Equals(*schema_));

  auto col = table_->column(0);
  ASSERT_EQ(schema_->field(0)->name(), col->name());
  ASSERT_EQ(schema_->field(0)->type(), col->type());
}

TEST_F(TestTable, InvalidColumns) {
  // Check that columns are all the same length
  const int length = 100;
  MakeExample1(length);

  table_ = Table::Make(schema_, columns_, length - 1);
  ASSERT_RAISES(Invalid, table_->Validate());

  columns_.clear();

  // Wrong number of columns
  table_ = Table::Make(schema_, columns_, length);
  ASSERT_RAISES(Invalid, table_->Validate());

  columns_ = {
      std::make_shared<Column>(schema_->field(0), MakeRandomArray<Int32Array>(length)),
      std::make_shared<Column>(schema_->field(1), MakeRandomArray<UInt8Array>(length)),
      std::make_shared<Column>(schema_->field(2),
                               MakeRandomArray<Int16Array>(length - 1))};

  table_ = Table::Make(schema_, columns_, length);
  ASSERT_RAISES(Invalid, table_->Validate());
}

TEST_F(TestTable, Equals) {
  const int length = 100;
  MakeExample1(length);

  table_ = Table::Make(schema_, columns_);

  ASSERT_TRUE(table_->Equals(*table_));
  // Differing schema
  auto f0 = field("f3", int32());
  auto f1 = field("f4", uint8());
  auto f2 = field("f5", int16());
  vector<shared_ptr<Field>> fields = {f0, f1, f2};
  auto other_schema = std::make_shared<Schema>(fields);
  auto other = Table::Make(other_schema, columns_);
  ASSERT_FALSE(table_->Equals(*other));
  // Differing columns
  std::vector<std::shared_ptr<Column>> other_columns = {
      std::make_shared<Column>(schema_->field(0),
                               MakeRandomArray<Int32Array>(length, 10)),
      std::make_shared<Column>(schema_->field(1),
                               MakeRandomArray<UInt8Array>(length, 10)),
      std::make_shared<Column>(schema_->field(2),
                               MakeRandomArray<Int16Array>(length, 10))};

  other = Table::Make(schema_, other_columns);
  ASSERT_FALSE(table_->Equals(*other));
}

TEST_F(TestTable, FromRecordBatches) {
  const int64_t length = 10;
  MakeExample1(length);

  auto batch1 = RecordBatch::Make(schema_, length, arrays_);

  std::shared_ptr<Table> result, expected;
  ASSERT_OK(Table::FromRecordBatches({batch1}, &result));

  expected = Table::Make(schema_, columns_);
  ASSERT_TRUE(result->Equals(*expected));

  std::vector<std::shared_ptr<Column>> other_columns;
  for (int i = 0; i < schema_->num_fields(); ++i) {
    std::vector<std::shared_ptr<Array>> col_arrays = {arrays_[i], arrays_[i]};
    other_columns.push_back(std::make_shared<Column>(schema_->field(i), col_arrays));
  }

  ASSERT_OK(Table::FromRecordBatches({batch1, batch1}, &result));
  expected = Table::Make(schema_, other_columns);
  ASSERT_TRUE(result->Equals(*expected));

  // Error states
  std::vector<std::shared_ptr<RecordBatch>> empty_batches;
  ASSERT_RAISES(Invalid, Table::FromRecordBatches(empty_batches, &result));

  auto other_schema = ::arrow::schema({schema_->field(0), schema_->field(1)});

  std::vector<std::shared_ptr<Array>> other_arrays = {arrays_[0], arrays_[1]};
  auto batch2 = RecordBatch::Make(other_schema, length, other_arrays);
  ASSERT_RAISES(Invalid, Table::FromRecordBatches({batch1, batch2}, &result));
}

TEST_F(TestTable, FromRecordBatchesZeroLength) {
  // ARROW-2307
  MakeExample1(10);

  std::shared_ptr<Table> result;
  ASSERT_OK(Table::FromRecordBatches(schema_, {}, &result));

  ASSERT_EQ(0, result->num_rows());
  ASSERT_TRUE(result->schema()->Equals(*schema_));
}

TEST_F(TestTable, ConcatenateTables) {
  const int64_t length = 10;

  MakeExample1(length);
  auto batch1 = RecordBatch::Make(schema_, length, arrays_);

  // generate different data
  MakeExample1(length);
  auto batch2 = RecordBatch::Make(schema_, length, arrays_);

  std::shared_ptr<Table> t1, t2, t3, result, expected;
  ASSERT_OK(Table::FromRecordBatches({batch1}, &t1));
  ASSERT_OK(Table::FromRecordBatches({batch2}, &t2));

  ASSERT_OK(ConcatenateTables({t1, t2}, &result));
  ASSERT_OK(Table::FromRecordBatches({batch1, batch2}, &expected));
  AssertTablesEqual(*expected, *result);

  // Error states
  std::vector<std::shared_ptr<Table>> empty_tables;
  ASSERT_RAISES(Invalid, ConcatenateTables(empty_tables, &result));

  auto other_schema = ::arrow::schema({schema_->field(0), schema_->field(1)});

  std::vector<std::shared_ptr<Array>> other_arrays = {arrays_[0], arrays_[1]};
  auto batch3 = RecordBatch::Make(other_schema, length, other_arrays);
  ASSERT_OK(Table::FromRecordBatches({batch3}, &t3));

  ASSERT_RAISES(Invalid, ConcatenateTables({t1, t3}, &result));
}

TEST_F(TestTable, RemoveColumn) {
  const int64_t length = 10;
  MakeExample1(length);

  auto table_sp = Table::Make(schema_, columns_);
  const Table& table = *table_sp;

  std::shared_ptr<Table> result;
  ASSERT_OK(table.RemoveColumn(0, &result));

  auto ex_schema = ::arrow::schema({schema_->field(1), schema_->field(2)});
  std::vector<std::shared_ptr<Column>> ex_columns = {table.column(1), table.column(2)};

  auto expected = Table::Make(ex_schema, ex_columns);
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK(table.RemoveColumn(1, &result));
  ex_schema = ::arrow::schema({schema_->field(0), schema_->field(2)});
  ex_columns = {table.column(0), table.column(2)};

  expected = Table::Make(ex_schema, ex_columns);
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK(table.RemoveColumn(2, &result));
  ex_schema = ::arrow::schema({schema_->field(0), schema_->field(1)});
  ex_columns = {table.column(0), table.column(1)};
  expected = Table::Make(ex_schema, ex_columns);
  ASSERT_TRUE(result->Equals(*expected));
}

TEST_F(TestTable, SetColumn) {
  const int64_t length = 10;
  MakeExample1(length);

  auto table_sp = Table::Make(schema_, columns_);
  const Table& table = *table_sp;

  std::shared_ptr<Table> result;
  ASSERT_OK(table.SetColumn(0, table.column(1), &result));

  auto ex_schema =
      ::arrow::schema({schema_->field(1), schema_->field(1), schema_->field(2)});
  std::vector<std::shared_ptr<Column>> ex_columns = {table.column(1), table.column(1),
                                                     table.column(2)};

  auto expected = Table::Make(ex_schema, ex_columns);
  ASSERT_TRUE(result->Equals(*expected));
}

TEST_F(TestTable, RemoveColumnEmpty) {
  // ARROW-1865
  const int64_t length = 10;

  auto f0 = field("f0", int32());
  auto schema = ::arrow::schema({f0});
  auto a0 = MakeRandomArray<Int32Array>(length);

  auto table = Table::Make(schema, {std::make_shared<Column>(f0, a0)});

  std::shared_ptr<Table> empty;
  ASSERT_OK(table->RemoveColumn(0, &empty));

  ASSERT_EQ(table->num_rows(), empty->num_rows());

  std::shared_ptr<Table> added;
  ASSERT_OK(empty->AddColumn(0, table->column(0), &added));
  ASSERT_EQ(table->num_rows(), added->num_rows());
}

TEST_F(TestTable, AddColumn) {
  const int64_t length = 10;
  MakeExample1(length);

  auto table_sp = Table::Make(schema_, columns_);
  const Table& table = *table_sp;

  std::shared_ptr<Table> result;
  // Some negative tests with invalid index
  Status status = table.AddColumn(10, columns_[0], &result);
  ASSERT_TRUE(status.IsInvalid());
  status = table.AddColumn(4, columns_[0], &result);
  ASSERT_TRUE(status.IsInvalid());
  status = table.AddColumn(-1, columns_[0], &result);
  ASSERT_TRUE(status.IsInvalid());

  // Add column with wrong length
  auto longer_col = std::make_shared<Column>(schema_->field(0),
                                             MakeRandomArray<Int32Array>(length + 1));
  status = table.AddColumn(0, longer_col, &result);
  ASSERT_TRUE(status.IsInvalid());

  // Add column 0 in different places
  ASSERT_OK(table.AddColumn(0, columns_[0], &result));
  auto ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(0), schema_->field(1), schema_->field(2)});

  auto expected = Table::Make(
      ex_schema, {table.column(0), table.column(0), table.column(1), table.column(2)});
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK(table.AddColumn(1, columns_[0], &result));
  ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(0), schema_->field(1), schema_->field(2)});

  expected = Table::Make(
      ex_schema, {table.column(0), table.column(0), table.column(1), table.column(2)});
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK(table.AddColumn(2, columns_[0], &result));
  ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(1), schema_->field(0), schema_->field(2)});
  expected = Table::Make(
      ex_schema, {table.column(0), table.column(1), table.column(0), table.column(2)});
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK(table.AddColumn(3, columns_[0], &result));
  ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(1), schema_->field(2), schema_->field(0)});
  expected = Table::Make(
      ex_schema, {table.column(0), table.column(1), table.column(2), table.column(0)});
  ASSERT_TRUE(result->Equals(*expected));
}

class TestRecordBatch : public TestBase {};

TEST_F(TestRecordBatch, Equals) {
  const int length = 10;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());

  vector<shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = ::arrow::schema({f0, f1, f2});
  auto schema2 = ::arrow::schema({f0, f1});

  auto a0 = MakeRandomArray<Int32Array>(length);
  auto a1 = MakeRandomArray<UInt8Array>(length);
  auto a2 = MakeRandomArray<Int16Array>(length);

  auto b1 = RecordBatch::Make(schema, length, {a0, a1, a2});
  auto b3 = RecordBatch::Make(schema2, length, {a0, a1});
  auto b4 = RecordBatch::Make(schema, length, {a0, a1, a1});

  ASSERT_TRUE(b1->Equals(*b1));
  ASSERT_FALSE(b1->Equals(*b3));
  ASSERT_FALSE(b1->Equals(*b4));
}

TEST_F(TestRecordBatch, Validate) {
  const int length = 10;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());

  auto schema = ::arrow::schema({f0, f1, f2});

  auto a0 = MakeRandomArray<Int32Array>(length);
  auto a1 = MakeRandomArray<UInt8Array>(length);
  auto a2 = MakeRandomArray<Int16Array>(length);
  auto a3 = MakeRandomArray<Int16Array>(5);

  auto b1 = RecordBatch::Make(schema, length, {a0, a1, a2});

  ASSERT_OK(b1->Validate());

  // Length mismatch
  auto b2 = RecordBatch::Make(schema, length, {a0, a1, a3});
  ASSERT_RAISES(Invalid, b2->Validate());

  // Type mismatch
  auto b3 = RecordBatch::Make(schema, length, {a0, a1, a0});
  ASSERT_RAISES(Invalid, b3->Validate());
}

TEST_F(TestRecordBatch, Slice) {
  const int length = 10;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());

  vector<shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema(fields);

  auto a0 = MakeRandomArray<Int32Array>(length);
  auto a1 = MakeRandomArray<UInt8Array>(length);

  auto batch = RecordBatch::Make(schema, length, {a0, a1});

  auto batch_slice = batch->Slice(2);
  auto batch_slice2 = batch->Slice(1, 5);

  ASSERT_EQ(batch_slice->num_rows(), batch->num_rows() - 2);

  for (int i = 0; i < batch->num_columns(); ++i) {
    ASSERT_EQ(2, batch_slice->column(i)->offset());
    ASSERT_EQ(length - 2, batch_slice->column(i)->length());

    ASSERT_EQ(1, batch_slice2->column(i)->offset());
    ASSERT_EQ(5, batch_slice2->column(i)->length());
  }
}

TEST_F(TestRecordBatch, AddColumn) {
  const int length = 10;

  auto field1 = field("f1", int32());
  auto field2 = field("f2", uint8());
  auto field3 = field("f3", int16());

  auto schema1 = ::arrow::schema({field1, field2});
  auto schema2 = ::arrow::schema({field2, field3});
  auto schema3 = ::arrow::schema({field2});

  auto array1 = MakeRandomArray<Int32Array>(length);
  auto array2 = MakeRandomArray<UInt8Array>(length);
  auto array3 = MakeRandomArray<Int16Array>(length);

  auto batch1 = RecordBatch::Make(schema1, length, {array1, array2});
  auto batch2 = RecordBatch::Make(schema2, length, {array2, array3});
  auto batch3 = RecordBatch::Make(schema3, length, {array2});

  const RecordBatch& batch = *batch3;
  std::shared_ptr<RecordBatch> result;

  // Negative tests with invalid index
  Status status = batch.AddColumn(5, field1, array1, &result);
  ASSERT_TRUE(status.IsInvalid());
  status = batch.AddColumn(2, field1, array1, &result);
  ASSERT_TRUE(status.IsInvalid());
  status = batch.AddColumn(-1, field1, array1, &result);
  ASSERT_TRUE(status.IsInvalid());

  // Negative test with wrong length
  auto longer_col = MakeRandomArray<Int32Array>(length + 1);
  status = batch.AddColumn(0, field1, longer_col, &result);
  ASSERT_TRUE(status.IsInvalid());

  // Negative test with mismatch type
  status = batch.AddColumn(0, field1, array2, &result);
  ASSERT_TRUE(status.IsInvalid());

  ASSERT_OK(batch.AddColumn(0, field1, array1, &result));
  ASSERT_TRUE(result->Equals(*batch1));

  ASSERT_OK(batch.AddColumn(1, field3, array3, &result));
  ASSERT_TRUE(result->Equals(*batch2));

  std::shared_ptr<RecordBatch> result2;
  ASSERT_OK(batch.AddColumn(1, "f3", array3, &result2));
  ASSERT_TRUE(result2->Equals(*result));

  ASSERT_TRUE(result2->schema()->field(1)->nullable());
}

TEST_F(TestRecordBatch, RemoveColumn) {
  const int length = 10;

  auto field1 = field("f1", int32());
  auto field2 = field("f2", uint8());
  auto field3 = field("f3", int16());

  auto schema1 = ::arrow::schema({field1, field2, field3});
  auto schema2 = ::arrow::schema({field2, field3});
  auto schema3 = ::arrow::schema({field1, field3});
  auto schema4 = ::arrow::schema({field1, field2});

  auto array1 = MakeRandomArray<Int32Array>(length);
  auto array2 = MakeRandomArray<UInt8Array>(length);
  auto array3 = MakeRandomArray<Int16Array>(length);

  auto batch1 = RecordBatch::Make(schema1, length, {array1, array2, array3});
  auto batch2 = RecordBatch::Make(schema2, length, {array2, array3});
  auto batch3 = RecordBatch::Make(schema3, length, {array1, array3});
  auto batch4 = RecordBatch::Make(schema4, length, {array1, array2});

  const RecordBatch& batch = *batch1;
  std::shared_ptr<RecordBatch> result;

  // Negative tests with invalid index
  Status status = batch.RemoveColumn(3, &result);
  ASSERT_TRUE(status.IsInvalid());
  status = batch.RemoveColumn(-1, &result);
  ASSERT_TRUE(status.IsInvalid());

  ASSERT_OK(batch.RemoveColumn(0, &result));
  ASSERT_TRUE(result->Equals(*batch2));

  ASSERT_OK(batch.RemoveColumn(1, &result));
  ASSERT_TRUE(result->Equals(*batch3));

  ASSERT_OK(batch.RemoveColumn(2, &result));
  ASSERT_TRUE(result->Equals(*batch4));
}

TEST_F(TestRecordBatch, RemoveColumnEmpty) {
  const int length = 10;

  auto field1 = field("f1", int32());
  auto schema1 = ::arrow::schema({field1});
  auto array1 = MakeRandomArray<Int32Array>(length);
  auto batch1 = RecordBatch::Make(schema1, length, {array1});

  std::shared_ptr<RecordBatch> empty;
  ASSERT_OK(batch1->RemoveColumn(0, &empty));
  ASSERT_EQ(batch1->num_rows(), empty->num_rows());

  std::shared_ptr<RecordBatch> added;
  ASSERT_OK(empty->AddColumn(0, field1, array1, &added));
  ASSERT_TRUE(added->Equals(*batch1));
}

class TestTableBatchReader : public TestBase {};

TEST_F(TestTableBatchReader, ReadNext) {
  ArrayVector c1, c2;

  auto a1 = MakeRandomArray<Int32Array>(10);
  auto a2 = MakeRandomArray<Int32Array>(20);
  auto a3 = MakeRandomArray<Int32Array>(30);
  auto a4 = MakeRandomArray<Int32Array>(10);

  auto sch1 = arrow::schema({field("f1", int32()), field("f2", int32())});

  std::vector<std::shared_ptr<Column>> columns;

  std::shared_ptr<RecordBatch> batch;

  columns = {column(sch1->field(0), {a1, a4, a2}), column(sch1->field(1), {a2, a2})};
  auto t1 = Table::Make(sch1, columns);

  TableBatchReader i1(*t1);

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(10, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(10, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(20, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(nullptr, batch);

  columns = {column(sch1->field(0), {a1}), column(sch1->field(1), {a4})};
  auto t2 = Table::Make(sch1, columns);

  TableBatchReader i2(*t2);

  ASSERT_OK(i2.ReadNext(&batch));
  ASSERT_EQ(10, batch->num_rows());

  // Ensure non-sliced
  ASSERT_EQ(a1->data().get(), batch->column_data(0).get());
  ASSERT_EQ(a4->data().get(), batch->column_data(1).get());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(nullptr, batch);
}

TEST_F(TestTableBatchReader, Chunksize) {
  auto a1 = MakeRandomArray<Int32Array>(10);
  auto a2 = MakeRandomArray<Int32Array>(20);
  auto a3 = MakeRandomArray<Int32Array>(10);

  auto sch1 = arrow::schema({field("f1", int32())});
  auto t1 = Table::Make(sch1, {column(sch1->field(0), {a1, a2, a3})});

  TableBatchReader i1(*t1);

  i1.set_chunksize(15);

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_OK(batch->Validate());
  ASSERT_EQ(10, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_OK(batch->Validate());
  ASSERT_EQ(15, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_OK(batch->Validate());
  ASSERT_EQ(5, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_OK(batch->Validate());
  ASSERT_EQ(10, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(nullptr, batch);
}

}  // namespace arrow
