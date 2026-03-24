//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/constant_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct ConstantVector {
	template <class T>
	static void VerifyVectorType(const Vector &vector) {
#ifdef DUCKDB_DEBUG_NO_SAFETY
		D_ASSERT(StorageTypeCompatible<T>(vector.GetType().InternalType()));
#else
		if (!StorageTypeCompatible<T>(vector.GetType().InternalType())) {
			throw InternalException("Expected vector of type %s, but found vector of type %s", GetTypeId<T>(),
			                        vector.GetType().InternalType());
		}
#endif
	}

	static inline const_data_ptr_t GetData(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR ||
		         vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return vector.buffer ? vector.buffer->GetData() : nullptr;
	}
	static inline data_ptr_t GetData(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR ||
		         vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return vector.buffer ? vector.buffer->GetData() : nullptr;
	}
	template <class T>
	static inline const T *GetDataUnsafe(const Vector &vector) {
		return reinterpret_cast<const T *>(GetData(vector));
	}
	template <class T>
	static inline T *GetDataUnsafe(Vector &vector) {
		return reinterpret_cast<T *>(GetData(vector));
	}
	template <class T>
	static inline const T *GetData(const Vector &vector) {
		VerifyVectorType<T>(vector);
		return GetDataUnsafe<T>(vector);
	}
	template <class T>
	static inline T *GetData(Vector &vector) {
		VerifyVectorType<T>(vector);
		return GetDataUnsafe<T>(vector);
	}
	static inline bool IsNull(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		return !vector.validity.RowIsValid(0);
	}
	DUCKDB_API static void SetNull(Vector &vector, bool is_null);
	static inline ValidityMask &Validity(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		return vector.validity;
	}
	DUCKDB_API static const SelectionVector *ZeroSelectionVector(idx_t count, SelectionVector &owned_sel);
	DUCKDB_API static const SelectionVector *ZeroSelectionVector();
	//! Turns "vector" into a constant vector by referencing a value within the source vector
	DUCKDB_API static void Reference(Vector &vector, Vector &source, idx_t position, idx_t count);
	//! Flatten a constant vector into a flat vector, repeating the const element according to the given count
	DUCKDB_API static void Flatten(const Vector &const_vector, Vector &result, idx_t count);

	static const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE];
};

} // namespace duckdb
