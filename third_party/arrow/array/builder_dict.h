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

#pragma once

#include <memory>

#include "arrow/array/builder_adaptive.h"  // IWYU pragma: export
#include "arrow/array/builder_base.h"      // IWYU pragma: export

namespace arrow {

// ----------------------------------------------------------------------
// Dictionary builder

namespace internal {

template <typename T>
struct DictionaryScalar {
  using type = typename T::c_type;
};

template <>
struct DictionaryScalar<BinaryType> {
  using type = util::string_view;
};

template <>
struct DictionaryScalar<StringType> {
  using type = util::string_view;
};

template <>
struct DictionaryScalar<FixedSizeBinaryType> {
  using type = util::string_view;
};

}  // namespace internal

/// \brief Array builder for created encoded DictionaryArray from dense array
///
/// Unlike other builders, dictionary builder does not completely reset the state
/// on Finish calls. The arrays built after the initial Finish call will reuse
/// the previously created encoding and build a delta dictionary when new terms
/// occur.
///
/// data
template <typename T>
class ARROW_EXPORT DictionaryBuilder : public ArrayBuilder {
 public:
  using Scalar = typename internal::DictionaryScalar<T>::type;

  // WARNING: the type given below is the value type, not the DictionaryType.
  // The DictionaryType is instantiated on the Finish() call.
  DictionaryBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool);

  template <typename T1 = T>
  explicit DictionaryBuilder(
      typename std::enable_if<TypeTraits<T1>::is_parameter_free, MemoryPool*>::type pool)
      : DictionaryBuilder<T1>(TypeTraits<T1>::type_singleton(), pool) {}

  ~DictionaryBuilder() override;

  /// \brief Append a scalar value
  Status Append(const Scalar& value);

  /// \brief Append a fixed-width string (only for FixedSizeBinaryType)
  template <typename T1 = T>
  Status Append(typename std::enable_if<std::is_base_of<FixedSizeBinaryType, T1>::value,
                                        const uint8_t*>::type value) {
    return Append(util::string_view(reinterpret_cast<const char*>(value), byte_width_));
  }

  /// \brief Append a fixed-width string (only for FixedSizeBinaryType)
  template <typename T1 = T>
  Status Append(typename std::enable_if<std::is_base_of<FixedSizeBinaryType, T1>::value,
                                        const char*>::type value) {
    return Append(util::string_view(value, byte_width_));
  }

  /// \brief Append a scalar null value
  Status AppendNull();

  /// \brief Append a whole dense array to the builder
  Status AppendArray(const Array& array);

  void Reset() override;
  Status Resize(int64_t capacity) override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// is the dictionary builder in the delta building mode
  bool is_building_delta() { return delta_offset_ > 0; }

 protected:
  class MemoTableImpl;
  std::unique_ptr<MemoTableImpl> memo_table_;

  int32_t delta_offset_;
  // Only used for FixedSizeBinaryType
  int32_t byte_width_;

  AdaptiveIntBuilder values_builder_;
};

template <>
class ARROW_EXPORT DictionaryBuilder<NullType> : public ArrayBuilder {
 public:
  DictionaryBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool);
  explicit DictionaryBuilder(MemoryPool* pool);

  /// \brief Append a scalar null value
  Status AppendNull();

  /// \brief Append a whole dense array to the builder
  Status AppendArray(const Array& array);

  Status Resize(int64_t capacity) override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

 protected:
  AdaptiveIntBuilder values_builder_;
};

class ARROW_EXPORT BinaryDictionaryBuilder : public DictionaryBuilder<BinaryType> {
 public:
  using DictionaryBuilder::Append;
  using DictionaryBuilder::DictionaryBuilder;

  Status Append(const uint8_t* value, int32_t length) {
    return Append(reinterpret_cast<const char*>(value), length);
  }

  Status Append(const char* value, int32_t length) {
    return Append(util::string_view(value, length));
  }
};

/// \brief Dictionary array builder with convenience methods for strings
class ARROW_EXPORT StringDictionaryBuilder : public DictionaryBuilder<StringType> {
 public:
  using DictionaryBuilder::Append;
  using DictionaryBuilder::DictionaryBuilder;

  Status Append(const uint8_t* value, int32_t length) {
    return Append(reinterpret_cast<const char*>(value), length);
  }

  Status Append(const char* value, int32_t length) {
    return Append(util::string_view(value, length));
  }
};

}  // namespace arrow
