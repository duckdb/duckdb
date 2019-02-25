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

// Private header, not to be exported

#ifndef ARROW_VISITOR_INLINE_H
#define ARROW_VISITOR_INLINE_H

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/string_view.h"

namespace arrow {

#define TYPE_VISIT_INLINE(TYPE_CLASS) \
  case TYPE_CLASS::type_id:           \
    return visitor->Visit(internal::checked_cast<const TYPE_CLASS&>(type));

template <typename VISITOR>
inline Status VisitTypeInline(const DataType& type, VISITOR* visitor) {
  switch (type.id()) {
    TYPE_VISIT_INLINE(NullType);
    TYPE_VISIT_INLINE(BooleanType);
    TYPE_VISIT_INLINE(Int8Type);
    TYPE_VISIT_INLINE(UInt8Type);
    TYPE_VISIT_INLINE(Int16Type);
    TYPE_VISIT_INLINE(UInt16Type);
    TYPE_VISIT_INLINE(Int32Type);
    TYPE_VISIT_INLINE(UInt32Type);
    TYPE_VISIT_INLINE(Int64Type);
    TYPE_VISIT_INLINE(UInt64Type);
    TYPE_VISIT_INLINE(HalfFloatType);
    TYPE_VISIT_INLINE(FloatType);
    TYPE_VISIT_INLINE(DoubleType);
    TYPE_VISIT_INLINE(StringType);
    TYPE_VISIT_INLINE(BinaryType);
    TYPE_VISIT_INLINE(FixedSizeBinaryType);
    TYPE_VISIT_INLINE(Date32Type);
    TYPE_VISIT_INLINE(Date64Type);
    TYPE_VISIT_INLINE(TimestampType);
    TYPE_VISIT_INLINE(Time32Type);
    TYPE_VISIT_INLINE(Time64Type);
    TYPE_VISIT_INLINE(Decimal128Type);
    TYPE_VISIT_INLINE(ListType);
    TYPE_VISIT_INLINE(StructType);
    TYPE_VISIT_INLINE(UnionType);
    TYPE_VISIT_INLINE(DictionaryType);
    default:
      break;
  }
  return Status::NotImplemented("Type not implemented");
}

#undef TYPE_VISIT_INLINE

#define ARRAY_VISIT_INLINE(TYPE_CLASS)                                             \
  case TYPE_CLASS::type_id:                                                        \
    return visitor->Visit(                                                         \
        internal::checked_cast<const typename TypeTraits<TYPE_CLASS>::ArrayType&>( \
            array));

template <typename VISITOR>
inline Status VisitArrayInline(const Array& array, VISITOR* visitor) {
  switch (array.type_id()) {
    ARRAY_VISIT_INLINE(NullType);
    ARRAY_VISIT_INLINE(BooleanType);
    ARRAY_VISIT_INLINE(Int8Type);
    ARRAY_VISIT_INLINE(UInt8Type);
    ARRAY_VISIT_INLINE(Int16Type);
    ARRAY_VISIT_INLINE(UInt16Type);
    ARRAY_VISIT_INLINE(Int32Type);
    ARRAY_VISIT_INLINE(UInt32Type);
    ARRAY_VISIT_INLINE(Int64Type);
    ARRAY_VISIT_INLINE(UInt64Type);
    ARRAY_VISIT_INLINE(HalfFloatType);
    ARRAY_VISIT_INLINE(FloatType);
    ARRAY_VISIT_INLINE(DoubleType);
    ARRAY_VISIT_INLINE(StringType);
    ARRAY_VISIT_INLINE(BinaryType);
    ARRAY_VISIT_INLINE(FixedSizeBinaryType);
    ARRAY_VISIT_INLINE(Date32Type);
    ARRAY_VISIT_INLINE(Date64Type);
    ARRAY_VISIT_INLINE(TimestampType);
    ARRAY_VISIT_INLINE(Time32Type);
    ARRAY_VISIT_INLINE(Time64Type);
    ARRAY_VISIT_INLINE(Decimal128Type);
    ARRAY_VISIT_INLINE(ListType);
    ARRAY_VISIT_INLINE(StructType);
    ARRAY_VISIT_INLINE(UnionType);
    ARRAY_VISIT_INLINE(DictionaryType);
    default:
      break;
  }
  return Status::NotImplemented("Type not implemented");
}

// Visit an array's data values, in order, without overhead.
//
// The Visit function's `visitor` argument should define two public methods:
// - Status VisitNull()
// - Status VisitValue(<scalar>)
//
// The scalar value's type depends on the array data type:
// - the type's `c_type`, if any
// - for boolean arrays, a `bool`
// - for binary, string and fixed-size binary arrays, a `util::string_view`

template <typename T, typename Enable = void>
struct ArrayDataVisitor {};

template <>
struct ArrayDataVisitor<BooleanType> {
  template <typename Visitor>
  static Status Visit(const ArrayData& arr, Visitor* visitor) {
    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      internal::BitmapReader value_reader(arr.buffers[1]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        if (is_null) {
          ARROW_RETURN_NOT_OK(visitor->VisitNull());
        } else {
          ARROW_RETURN_NOT_OK(visitor->VisitValue(value_reader.IsSet()));
        }
        valid_reader.Next();
        value_reader.Next();
      }
    } else {
      internal::BitmapReader value_reader(arr.buffers[1]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        ARROW_RETURN_NOT_OK(visitor->VisitValue(value_reader.IsSet()));
        value_reader.Next();
      }
    }
    return Status::OK();
  }
};

template <typename T>
struct ArrayDataVisitor<T, enable_if_has_c_type<T>> {
  template <typename Visitor>
  static Status Visit(const ArrayData& arr, Visitor* visitor) {
    using c_type = typename T::c_type;
    const c_type* data = arr.GetValues<c_type>(1);

    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        if (is_null) {
          ARROW_RETURN_NOT_OK(visitor->VisitNull());
        } else {
          ARROW_RETURN_NOT_OK(visitor->VisitValue(data[i]));
        }
        valid_reader.Next();
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        ARROW_RETURN_NOT_OK(visitor->VisitValue(data[i]));
      }
    }
    return Status::OK();
  }
};

template <typename T>
struct ArrayDataVisitor<T, enable_if_binary<T>> {
  template <typename Visitor>
  static Status Visit(const ArrayData& arr, Visitor* visitor) {
    constexpr uint8_t empty_value = 0;

    const int32_t* offsets = arr.GetValues<int32_t>(1);
    const uint8_t* data;
    if (!arr.buffers[2]) {
      data = &empty_value;
    } else {
      data = arr.GetValues<uint8_t>(2);
    }

    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        valid_reader.Next();
        if (is_null) {
          ARROW_RETURN_NOT_OK(visitor->VisitNull());
        } else {
          auto value = util::string_view(reinterpret_cast<const char*>(data + offsets[i]),
                                         offsets[i + 1] - offsets[i]);
          ARROW_RETURN_NOT_OK(visitor->VisitValue(value));
        }
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        auto value = util::string_view(reinterpret_cast<const char*>(data + offsets[i]),
                                       offsets[i + 1] - offsets[i]);
        ARROW_RETURN_NOT_OK(visitor->VisitValue(value));
      }
    }
    return Status::OK();
  }
};

template <typename T>
struct ArrayDataVisitor<T, enable_if_fixed_size_binary<T>> {
  template <typename Visitor>
  static Status Visit(const ArrayData& arr, Visitor* visitor) {
    const auto& fw_type = internal::checked_cast<const FixedSizeBinaryType&>(*arr.type);

    const int32_t byte_width = fw_type.byte_width();
    const uint8_t* data = arr.GetValues<uint8_t>(1);

    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        valid_reader.Next();
        if (is_null) {
          ARROW_RETURN_NOT_OK(visitor->VisitNull());
        } else {
          auto value = util::string_view(reinterpret_cast<const char*>(data), byte_width);
          ARROW_RETURN_NOT_OK(visitor->VisitValue(value));
        }
        data += byte_width;
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        auto value = util::string_view(reinterpret_cast<const char*>(data), byte_width);
        ARROW_RETURN_NOT_OK(visitor->VisitValue(value));
        data += byte_width;
      }
    }
    return Status::OK();
  }
};

}  // namespace arrow

#endif  // ARROW_VISITOR_INLINE_H
