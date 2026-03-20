//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/fsst_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct FSSTVector {
	static inline const ValidityMask &Validity(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return vector.validity;
	}
	static inline ValidityMask &Validity(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return vector.validity;
	}
	static inline void SetValidity(Vector &vector, ValidityMask &new_validity) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		vector.validity.Initialize(new_validity);
	}
	static inline const_data_ptr_t GetCompressedData(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return vector.data;
	}
	static inline data_ptr_t GetCompressedData(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return vector.data;
	}
	template <class T>
	static inline const T *GetCompressedData(const Vector &vector) {
		return (const T *)FSSTVector::GetCompressedData(vector);
	}
	template <class T>
	static inline T *GetCompressedData(Vector &vector) {
		return (T *)FSSTVector::GetCompressedData(vector);
	}
	//! Decompresses an FSST_VECTOR into a FLAT_VECTOR. Note: validity is not copied.
	static void DecompressVector(const Vector &src, Vector &dst, idx_t src_offset, idx_t dst_offset, idx_t copy_count,
	                             const SelectionVector *sel);

	DUCKDB_API static string_t AddCompressedString(Vector &vector, string_t data);
	DUCKDB_API static string_t AddCompressedString(Vector &vector, const char *data, idx_t len);
	DUCKDB_API static void RegisterDecoder(Vector &vector, buffer_ptr<void> &duckdb_fsst_decoder,
	                                       const idx_t string_block_limit);
	DUCKDB_API static void *GetDecoder(const Vector &vector);
	DUCKDB_API static vector<unsigned char> &GetDecompressBuffer(const Vector &vector);
	//! Setting the string count is required to be able to correctly flatten the vector
	DUCKDB_API static void SetCount(Vector &vector, idx_t count);
	DUCKDB_API static idx_t GetCount(Vector &vector);
};

}
