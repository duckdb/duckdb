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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/builder.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table_builder.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

class Array;

using internal::checked_cast;

class TestRecordBatchBuilder : public TestBase {
 public:
};

std::shared_ptr<Schema> ExampleSchema1() {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", utf8());
  auto f2 = field("f1", list(int8()));
  return ::arrow::schema({f0, f1, f2});
}

template <typename BuilderType, typename T>
void AppendValues(BuilderType* builder, const std::vector<T>& values,
                  const std::vector<bool>& is_valid) {
  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid.size() == 0 || is_valid[i]) {
      ASSERT_OK(builder->Append(values[i]));
    } else {
      ASSERT_OK(builder->AppendNull());
    }
  }
}

template <typename ValueType, typename T>
void AppendList(ListBuilder* builder, const std::vector<std::vector<T>>& values,
                const std::vector<bool>& is_valid) {
  auto values_builder = checked_cast<ValueType*>(builder->value_builder());

  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid.size() == 0 || is_valid[i]) {
      ASSERT_OK(builder->Append());
      AppendValues<ValueType, T>(values_builder, values[i], {});
    } else {
      ASSERT_OK(builder->AppendNull());
    }
  }
}

TEST_F(TestRecordBatchBuilder, Basics) {
  auto schema = ExampleSchema1();

  std::unique_ptr<RecordBatchBuilder> builder;
  ASSERT_OK(RecordBatchBuilder::Make(schema, pool_, &builder));

  std::vector<bool> is_valid = {false, true, true, true};
  std::vector<int32_t> f0_values = {0, 1, 2, 3};
  std::vector<std::string> f1_values = {"a", "bb", "ccc", "dddd"};
  std::vector<std::vector<int8_t>> f2_values = {{}, {0, 1}, {}, {2}};

  std::shared_ptr<Array> a0, a1, a2;

  // Make the expected record batch
  auto AppendData = [&](Int32Builder* b0, StringBuilder* b1, ListBuilder* b2) {
    AppendValues<Int32Builder, int32_t>(b0, f0_values, is_valid);
    AppendValues<StringBuilder, std::string>(b1, f1_values, is_valid);
    AppendList<Int8Builder, int8_t>(b2, f2_values, is_valid);
  };

  Int32Builder ex_b0;
  StringBuilder ex_b1;
  ListBuilder ex_b2(pool_, std::unique_ptr<Int8Builder>(new Int8Builder(pool_)));

  AppendData(&ex_b0, &ex_b1, &ex_b2);
  ASSERT_OK(ex_b0.Finish(&a0));
  ASSERT_OK(ex_b1.Finish(&a1));
  ASSERT_OK(ex_b2.Finish(&a2));

  auto expected = RecordBatch::Make(schema, 4, {a0, a1, a2});

  // Builder attributes
  ASSERT_EQ(3, builder->num_fields());
  ASSERT_EQ(schema.get(), builder->schema().get());

  const int kIter = 3;
  for (int i = 0; i < kIter; ++i) {
    AppendData(builder->GetFieldAs<Int32Builder>(0),
               checked_cast<StringBuilder*>(builder->GetField(1)),
               builder->GetFieldAs<ListBuilder>(2));

    std::shared_ptr<RecordBatch> batch;

    if (i == kIter - 1) {
      // Do not flush in last iteration
      ASSERT_OK(builder->Flush(false, &batch));
    } else {
      ASSERT_OK(builder->Flush(&batch));
    }

    ASSERT_BATCHES_EQUAL(*expected, *batch);
  }

  // Test setting initial capacity
  builder->SetInitialCapacity(4096);
  ASSERT_EQ(4096, builder->initial_capacity());
}

TEST_F(TestRecordBatchBuilder, InvalidFieldLength) {
  auto schema = ExampleSchema1();

  std::unique_ptr<RecordBatchBuilder> builder;
  ASSERT_OK(RecordBatchBuilder::Make(schema, pool_, &builder));

  std::vector<bool> is_valid = {false, true, true, true};
  std::vector<int32_t> f0_values = {0, 1, 2, 3};

  AppendValues<Int32Builder, int32_t>(builder->GetFieldAs<Int32Builder>(0), f0_values,
                                      is_valid);

  std::shared_ptr<RecordBatch> dummy;
  ASSERT_RAISES(Invalid, builder->Flush(&dummy));
}

}  // namespace arrow
