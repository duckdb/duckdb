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

//! The DictionaryBuffer holds a selection vector
class DictionaryBuffer : public VectorBuffer {
public:
	explicit DictionaryBuffer(const SelectionVector &sel)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(sel) {
	}
	explicit DictionaryBuffer(buffer_ptr<SelectionData> data)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(std::move(data)) {
	}
	explicit DictionaryBuffer(idx_t count = STANDARD_VECTOR_SIZE)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(count) {
	}

public:
	const SelectionVector &GetSelVector() const {
		return sel_vector;
	}
	SelectionVector &GetSelVector() {
		return sel_vector;
	}
	void SetSelVector(const SelectionVector &vector) {
		this->sel_vector.Initialize(vector);
	}
	void SetDictionarySize(idx_t dict_size) {
		dictionary_size = dict_size;
	}
	optional_idx GetDictionarySize() const {
		return dictionary_size;
	}
	void SetDictionaryId(string id) {
		dictionary_id = std::move(id);
	}
	const string &GetDictionaryId() const {
		return dictionary_id;
	}

private:
	SelectionVector sel_vector;
	optional_idx dictionary_size;
	//! A unique identifier for the dictionary that can be used to check if two dictionaries are equivalent
	string dictionary_id;
};

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
