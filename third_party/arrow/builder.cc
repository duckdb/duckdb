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

#include "arrow/builder.h"

#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

class MemoryPool;

// ----------------------------------------------------------------------
// Helper functions

#define BUILDER_CASE(ENUM, BuilderType)      \
  case Type::ENUM:                           \
    out->reset(new BuilderType(type, pool)); \
    return Status::OK();

// Initially looked at doing this with vtables, but shared pointers makes it
// difficult
//
// TODO(wesm): come up with a less monolithic strategy
Status MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                   std::unique_ptr<ArrayBuilder>* out) {
  switch (type->id()) {
    case Type::NA: {
      out->reset(new NullBuilder(pool));
      return Status::OK();
    }
      BUILDER_CASE(UINT8, UInt8Builder);
      BUILDER_CASE(INT8, Int8Builder);
      BUILDER_CASE(UINT16, UInt16Builder);
      BUILDER_CASE(INT16, Int16Builder);
      BUILDER_CASE(UINT32, UInt32Builder);
      BUILDER_CASE(INT32, Int32Builder);
      BUILDER_CASE(UINT64, UInt64Builder);
      BUILDER_CASE(INT64, Int64Builder);
      BUILDER_CASE(DATE32, Date32Builder);
      BUILDER_CASE(DATE64, Date64Builder);
      BUILDER_CASE(TIME32, Time32Builder);
      BUILDER_CASE(TIME64, Time64Builder);
      BUILDER_CASE(TIMESTAMP, TimestampBuilder);
      BUILDER_CASE(BOOL, BooleanBuilder);
      BUILDER_CASE(HALF_FLOAT, HalfFloatBuilder);
      BUILDER_CASE(FLOAT, FloatBuilder);
      BUILDER_CASE(DOUBLE, DoubleBuilder);
      BUILDER_CASE(STRING, StringBuilder);
      BUILDER_CASE(BINARY, BinaryBuilder);
      BUILDER_CASE(FIXED_SIZE_BINARY, FixedSizeBinaryBuilder);
      BUILDER_CASE(DECIMAL, Decimal128Builder);
    case Type::LIST: {
      std::unique_ptr<ArrayBuilder> value_builder;
      std::shared_ptr<DataType> value_type =
          internal::checked_cast<const ListType&>(*type).value_type();
      RETURN_NOT_OK(MakeBuilder(pool, value_type, &value_builder));
      out->reset(new ListBuilder(pool, std::move(value_builder), type));
      return Status::OK();
    }

    case Type::STRUCT: {
      const std::vector<std::shared_ptr<Field>>& fields = type->children();
      std::vector<std::shared_ptr<ArrayBuilder>> values_builder;

      for (auto it : fields) {
        std::unique_ptr<ArrayBuilder> builder;
        RETURN_NOT_OK(MakeBuilder(pool, it->type(), &builder));
        values_builder.emplace_back(std::move(builder));
      }
      out->reset(new StructBuilder(type, pool, std::move(values_builder)));
      return Status::OK();
    }

    default: {
      return Status::NotImplemented("MakeBuilder: cannot construct builder for type ",
                                    type->ToString());
    }
  }
}

}  // namespace arrow
