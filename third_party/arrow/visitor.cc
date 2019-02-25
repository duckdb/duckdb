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

#include "arrow/visitor.h"

#include <memory>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {

#define ARRAY_VISITOR_DEFAULT(ARRAY_CLASS)                   \
  Status ArrayVisitor::Visit(const ARRAY_CLASS& array) {     \
    return Status::NotImplemented(array.type()->ToString()); \
  }

ARRAY_VISITOR_DEFAULT(NullArray)
ARRAY_VISITOR_DEFAULT(BooleanArray)
ARRAY_VISITOR_DEFAULT(Int8Array)
ARRAY_VISITOR_DEFAULT(Int16Array)
ARRAY_VISITOR_DEFAULT(Int32Array)
ARRAY_VISITOR_DEFAULT(Int64Array)
ARRAY_VISITOR_DEFAULT(UInt8Array)
ARRAY_VISITOR_DEFAULT(UInt16Array)
ARRAY_VISITOR_DEFAULT(UInt32Array)
ARRAY_VISITOR_DEFAULT(UInt64Array)
ARRAY_VISITOR_DEFAULT(HalfFloatArray)
ARRAY_VISITOR_DEFAULT(FloatArray)
ARRAY_VISITOR_DEFAULT(DoubleArray)
ARRAY_VISITOR_DEFAULT(BinaryArray)
ARRAY_VISITOR_DEFAULT(StringArray)
ARRAY_VISITOR_DEFAULT(FixedSizeBinaryArray)
ARRAY_VISITOR_DEFAULT(Date32Array)
ARRAY_VISITOR_DEFAULT(Date64Array)
ARRAY_VISITOR_DEFAULT(Time32Array)
ARRAY_VISITOR_DEFAULT(Time64Array)
ARRAY_VISITOR_DEFAULT(TimestampArray)
ARRAY_VISITOR_DEFAULT(IntervalArray)
ARRAY_VISITOR_DEFAULT(ListArray)
ARRAY_VISITOR_DEFAULT(StructArray)
ARRAY_VISITOR_DEFAULT(UnionArray)
ARRAY_VISITOR_DEFAULT(DictionaryArray)
ARRAY_VISITOR_DEFAULT(Decimal128Array)

#undef ARRAY_VISITOR_DEFAULT

// ----------------------------------------------------------------------
// Default implementations of TypeVisitor methods

#define TYPE_VISITOR_DEFAULT(TYPE_CLASS)              \
  Status TypeVisitor::Visit(const TYPE_CLASS& type) { \
    return Status::NotImplemented(type.ToString());   \
  }

TYPE_VISITOR_DEFAULT(NullType)
TYPE_VISITOR_DEFAULT(BooleanType)
TYPE_VISITOR_DEFAULT(Int8Type)
TYPE_VISITOR_DEFAULT(Int16Type)
TYPE_VISITOR_DEFAULT(Int32Type)
TYPE_VISITOR_DEFAULT(Int64Type)
TYPE_VISITOR_DEFAULT(UInt8Type)
TYPE_VISITOR_DEFAULT(UInt16Type)
TYPE_VISITOR_DEFAULT(UInt32Type)
TYPE_VISITOR_DEFAULT(UInt64Type)
TYPE_VISITOR_DEFAULT(HalfFloatType)
TYPE_VISITOR_DEFAULT(FloatType)
TYPE_VISITOR_DEFAULT(DoubleType)
TYPE_VISITOR_DEFAULT(StringType)
TYPE_VISITOR_DEFAULT(BinaryType)
TYPE_VISITOR_DEFAULT(FixedSizeBinaryType)
TYPE_VISITOR_DEFAULT(Date64Type)
TYPE_VISITOR_DEFAULT(Date32Type)
TYPE_VISITOR_DEFAULT(Time32Type)
TYPE_VISITOR_DEFAULT(Time64Type)
TYPE_VISITOR_DEFAULT(TimestampType)
TYPE_VISITOR_DEFAULT(IntervalType)
TYPE_VISITOR_DEFAULT(Decimal128Type)
TYPE_VISITOR_DEFAULT(ListType)
TYPE_VISITOR_DEFAULT(StructType)
TYPE_VISITOR_DEFAULT(UnionType)
TYPE_VISITOR_DEFAULT(DictionaryType)

#undef TYPE_VISITOR_DEFAULT

}  // namespace arrow
