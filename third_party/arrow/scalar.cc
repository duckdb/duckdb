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

#include "arrow/scalar.h"

#include <memory>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

Time32Scalar::Time32Scalar(int32_t value, const std::shared_ptr<DataType>& type,
                           bool is_valid)
    : Scalar{type, is_valid}, value(value) {
  DCHECK_EQ(Type::TIME32, type->id());
}

Time64Scalar::Time64Scalar(int64_t value, const std::shared_ptr<DataType>& type,
                           bool is_valid)
    : Scalar{type, is_valid}, value(value) {
  DCHECK_EQ(Type::TIME64, type->id());
}

TimestampScalar::TimestampScalar(int64_t value, const std::shared_ptr<DataType>& type,
                                 bool is_valid)
    : Scalar{type, is_valid}, value(value) {
  DCHECK_EQ(Type::TIMESTAMP, type->id());
}

FixedSizeBinaryScalar::FixedSizeBinaryScalar(const std::shared_ptr<Buffer>& value,
                                             const std::shared_ptr<DataType>& type,
                                             bool is_valid)
    : BinaryScalar(value, type, is_valid) {
  DCHECK_EQ(checked_cast<const FixedSizeBinaryType&>(*type).byte_width(), value->size());
}

Decimal128Scalar::Decimal128Scalar(const Decimal128& value,
                                   const std::shared_ptr<DataType>& type, bool is_valid)
    : Scalar{type, is_valid}, value(value) {}

ListScalar::ListScalar(const std::shared_ptr<Array>& value,
                       const std::shared_ptr<DataType>& type, bool is_valid)
    : Scalar{type, is_valid}, value(value) {}

ListScalar::ListScalar(const std::shared_ptr<Array>& value, bool is_valid)
    : ListScalar(value, value->type(), is_valid) {}

}  // namespace arrow
