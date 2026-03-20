//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/dictionary_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct DictionaryVector {
	static void VerifyDictionary(const Vector &vector) {
#ifdef DUCKDB_DEBUG_NO_SAFETY
		D_ASSERT(vector.GetVectorType() == VectorType::DICTIONARY_VECTOR);
#else
		if (vector.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
			throw InternalException(
			    "Operation requires a dictionary vector but a non-dictionary vector was encountered");
		}
#endif
	}
	static inline const SelectionVector &SelVector(const Vector &vector) {
		VerifyDictionary(vector);
		return vector.buffer->Cast<DictionaryBuffer>().GetSelVector();
	}
	static inline SelectionVector &SelVector(Vector &vector) {
		VerifyDictionary(vector);
		return vector.buffer->Cast<DictionaryBuffer>().GetSelVector();
	}
	static inline const Vector &Child(const Vector &vector) {
		VerifyDictionary(vector);
		return vector.auxiliary->Cast<VectorChildBuffer>().data;
	}
	static inline Vector &Child(Vector &vector) {
		VerifyDictionary(vector);
		return vector.auxiliary->Cast<VectorChildBuffer>().data;
	}
	static inline optional_idx DictionarySize(const Vector &vector) {
		VerifyDictionary(vector);
		const auto &child_buffer = vector.auxiliary->Cast<VectorChildBuffer>();
		if (child_buffer.size.IsValid()) {
			return child_buffer.size;
		}
		return vector.buffer->Cast<DictionaryBuffer>().GetDictionarySize();
	}
	static inline const string &DictionaryId(const Vector &vector) {
		VerifyDictionary(vector);
		const auto &child_buffer = vector.auxiliary->Cast<VectorChildBuffer>();
		if (!child_buffer.id.empty()) {
			return child_buffer.id;
		}
		return vector.buffer->Cast<DictionaryBuffer>().GetDictionaryId();
	}
	static inline bool CanCacheHashes(const LogicalType &type) {
		return type.InternalType() == PhysicalType::VARCHAR;
	}
	static inline bool CanCacheHashes(const Vector &vector) {
		return DictionarySize(vector).IsValid() && CanCacheHashes(vector.GetType());
	}
	static buffer_ptr<VectorChildBuffer> CreateReusableDictionary(const LogicalType &type, const idx_t &size);
	static const Vector &GetCachedHashes(Vector &input);
};

} // namespace duckdb
