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

//! The DictionaryEntry holds a child Vector for dictionary-encoded vectors
class DictionaryEntry {
public:
	explicit DictionaryEntry(Vector vector) : data(std::move(vector)) {
	}

public:
	Vector data;
	//! Optional size/id to uniquely identify re-occurring dictionaries
	optional_idx size;
	string id;
	//! For caching the hashes of a child buffer
	mutex cached_hashes_lock;
	unique_ptr<Vector> cached_hashes;
};

//! The DictionaryBuffer holds a selection vector and a reference to a DictionaryEntry
class DictionaryBuffer : public VectorBuffer {
public:
	explicit DictionaryBuffer(const SelectionVector &sel, buffer_ptr<DictionaryEntry> entry_p);
	explicit DictionaryBuffer(buffer_ptr<SelectionData> data, buffer_ptr<DictionaryEntry> entry_p);
	explicit DictionaryBuffer(const SelectionVector &sel);
	explicit DictionaryBuffer(buffer_ptr<SelectionData> data);
	explicit DictionaryBuffer(idx_t count = STANDARD_VECTOR_SIZE);

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

	DictionaryEntry &GetEntry() {
		return *entry;
	}
	const DictionaryEntry &GetEntry() const {
		return *entry;
	}
	buffer_ptr<DictionaryEntry> GetEntryPtr() {
		return entry;
	}
	void SetEntry(buffer_ptr<DictionaryEntry> entry_p) {
		entry = std::move(entry_p);
	}

public:
	idx_t GetDataSize(const LogicalType &type, idx_t count) const override;
	idx_t GetAllocationSize() const override;
	void ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const override;
	buffer_ptr<VectorBuffer> Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;
	Value GetValue(const LogicalType &type, idx_t index) const override;
	void Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;
	buffer_ptr<VectorBuffer> SliceWithCache(SelCache &cache, const LogicalType &type, const SelectionVector &sel,
	                                        idx_t count) override;

protected:
	buffer_ptr<VectorBuffer> SliceInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) override;

private:
	SelectionVector sel_vector;
	buffer_ptr<DictionaryEntry> entry;
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
		return vector.Buffer().Cast<DictionaryBuffer>().GetSelVector();
	}
	static inline SelectionVector &SelVector(Vector &vector) {
		VerifyDictionary(vector);
		return vector.BufferMutable().Cast<DictionaryBuffer>().GetSelVector();
	}
	static inline const Vector &Child(const Vector &vector) {
		VerifyDictionary(vector);
		return vector.Buffer().Cast<DictionaryBuffer>().GetEntry().data;
	}
	static inline Vector &Child(Vector &vector) {
		VerifyDictionary(vector);
		return vector.BufferMutable().Cast<DictionaryBuffer>().GetEntry().data;
	}
	static inline optional_idx DictionarySize(const Vector &vector) {
		VerifyDictionary(vector);
		const auto &dict_buffer = vector.Buffer().Cast<DictionaryBuffer>();
		const auto &entry = dict_buffer.GetEntry();
		if (entry.size.IsValid()) {
			return entry.size;
		}
		return dict_buffer.GetDictionarySize();
	}
	static inline const string &DictionaryId(const Vector &vector) {
		VerifyDictionary(vector);
		const auto &dict_buffer = vector.Buffer().Cast<DictionaryBuffer>();
		const auto &entry = dict_buffer.GetEntry();
		if (!entry.id.empty()) {
			return entry.id;
		}
		return dict_buffer.GetDictionaryId();
	}
	static inline bool CanCacheHashes(const LogicalType &type) {
		return type.InternalType() == PhysicalType::VARCHAR;
	}
	static inline bool CanCacheHashes(const Vector &vector) {
		return DictionarySize(vector).IsValid() && CanCacheHashes(vector.GetType());
	}
	static buffer_ptr<DictionaryEntry> CreateReusableDictionary(const LogicalType &type, const idx_t &size);
	static const Vector &GetCachedHashes(Vector &input);
};

} // namespace duckdb
