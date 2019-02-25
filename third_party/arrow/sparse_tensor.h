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

#ifndef ARROW_SPARSE_TENSOR_H
#define ARROW_SPARSE_TENSOR_H

#include <memory>
#include <string>
#include <vector>

#include "arrow/tensor.h"

namespace arrow {

// ----------------------------------------------------------------------
// SparseIndex class

/// \brief EXPERIMENTAL: Sparse tensor format enumeration
struct SparseTensorFormat {
  enum type { COO, CSR };
};

/// \brief EXPERIMENTAL: The base class for representing index of non-zero
/// values in sparse tensor
class ARROW_EXPORT SparseIndex {
 public:
  explicit SparseIndex(SparseTensorFormat::type format_id, int64_t non_zero_length)
      : format_id_(format_id), non_zero_length_(non_zero_length) {}

  virtual ~SparseIndex() = default;

  SparseTensorFormat::type format_id() const { return format_id_; }
  int64_t non_zero_length() const { return non_zero_length_; }

  virtual std::string ToString() const = 0;

 protected:
  SparseTensorFormat::type format_id_;
  int64_t non_zero_length_;
};

template <typename SparseIndexType>
class SparseIndexBase : public SparseIndex {
 public:
  explicit SparseIndexBase(int64_t non_zero_length)
      : SparseIndex(SparseIndexType::format_id, non_zero_length) {}
};

// ----------------------------------------------------------------------
// SparseCOOIndex class

/// \brief EXPERIMENTAL: The index data for COO sparse tensor
class ARROW_EXPORT SparseCOOIndex : public SparseIndexBase<SparseCOOIndex> {
 public:
  using CoordsTensor = NumericTensor<Int64Type>;

  static constexpr SparseTensorFormat::type format_id = SparseTensorFormat::COO;

  // Constructor with a column-major NumericTensor
  explicit SparseCOOIndex(const std::shared_ptr<CoordsTensor>& coords);

  const std::shared_ptr<CoordsTensor>& indices() const { return coords_; }

  std::string ToString() const override;

  bool Equals(const SparseCOOIndex& other) const {
    return indices()->Equals(*other.indices());
  }

 protected:
  std::shared_ptr<CoordsTensor> coords_;
};

// ----------------------------------------------------------------------
// SparseCSRIndex class

/// \brief EXPERIMENTAL: The index data for CSR sparse matrix
class ARROW_EXPORT SparseCSRIndex : public SparseIndexBase<SparseCSRIndex> {
 public:
  using IndexTensor = NumericTensor<Int64Type>;

  static constexpr SparseTensorFormat::type format_id = SparseTensorFormat::CSR;

  // Constructor with two index vectors
  explicit SparseCSRIndex(const std::shared_ptr<IndexTensor>& indptr,
                          const std::shared_ptr<IndexTensor>& indices);

  const std::shared_ptr<IndexTensor>& indptr() const { return indptr_; }
  const std::shared_ptr<IndexTensor>& indices() const { return indices_; }

  std::string ToString() const override;

  bool Equals(const SparseCSRIndex& other) const {
    return indptr()->Equals(*other.indptr()) && indices()->Equals(*other.indices());
  }

 protected:
  std::shared_ptr<IndexTensor> indptr_;
  std::shared_ptr<IndexTensor> indices_;
};

// ----------------------------------------------------------------------
// SparseTensor class

/// \brief EXPERIMENTAL: The base class of sparse tensor container
class ARROW_EXPORT SparseTensor {
 public:
  virtual ~SparseTensor() = default;

  SparseTensorFormat::type format_id() const { return sparse_index_->format_id(); }

  std::shared_ptr<DataType> type() const { return type_; }
  std::shared_ptr<Buffer> data() const { return data_; }

  const uint8_t* raw_data() const { return data_->data(); }
  uint8_t* raw_mutable_data() const { return data_->mutable_data(); }

  const std::vector<int64_t>& shape() const { return shape_; }

  const std::shared_ptr<SparseIndex>& sparse_index() const { return sparse_index_; }

  int ndim() const { return static_cast<int>(shape_.size()); }

  const std::vector<std::string>& dim_names() const { return dim_names_; }
  const std::string& dim_name(int i) const;

  /// Total number of value cells in the sparse tensor
  int64_t size() const;

  /// Return true if the underlying data buffer is mutable
  bool is_mutable() const { return data_->is_mutable(); }

  /// Total number of non-zero cells in the sparse tensor
  int64_t non_zero_length() const {
    return sparse_index_ ? sparse_index_->non_zero_length() : 0;
  }

  bool Equals(const SparseTensor& other) const;

 protected:
  // Constructor with all attributes
  SparseTensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
               const std::vector<int64_t>& shape,
               const std::shared_ptr<SparseIndex>& sparse_index,
               const std::vector<std::string>& dim_names);

  std::shared_ptr<DataType> type_;
  std::shared_ptr<Buffer> data_;
  std::vector<int64_t> shape_;
  std::shared_ptr<SparseIndex> sparse_index_;

  /// These names are optional
  std::vector<std::string> dim_names_;
};

// ----------------------------------------------------------------------
// SparseTensorImpl class

/// \brief EXPERIMENTAL: Concrete sparse tensor implementation classes with sparse index
/// type
template <typename SparseIndexType>
class ARROW_EXPORT SparseTensorImpl : public SparseTensor {
 public:
  virtual ~SparseTensorImpl() = default;

  // Constructor with all attributes
  SparseTensorImpl(const std::shared_ptr<SparseIndexType>& sparse_index,
                   const std::shared_ptr<DataType>& type,
                   const std::shared_ptr<Buffer>& data, const std::vector<int64_t>& shape,
                   const std::vector<std::string>& dim_names)
      : SparseTensor(type, data, shape, sparse_index, dim_names) {}

  // Constructor for empty sparse tensor
  SparseTensorImpl(const std::shared_ptr<DataType>& type,
                   const std::vector<int64_t>& shape,
                   const std::vector<std::string>& dim_names = {});

  // Constructor with a dense numeric tensor
  template <typename TYPE>
  explicit SparseTensorImpl(const NumericTensor<TYPE>& tensor);

  // Constructor with a dense tensor
  explicit SparseTensorImpl(const Tensor& tensor);

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(SparseTensorImpl);
};

/// \brief EXPERIMENTAL: Type alias for COO sparse tensor
using SparseTensorCOO = SparseTensorImpl<SparseCOOIndex>;

/// \brief EXPERIMENTAL: Type alias for CSR sparse matrix
using SparseTensorCSR = SparseTensorImpl<SparseCSRIndex>;
using SparseMatrixCSR = SparseTensorImpl<SparseCSRIndex>;

}  // namespace arrow

#endif  // ARROW_SPARSE_TENSOR_H
