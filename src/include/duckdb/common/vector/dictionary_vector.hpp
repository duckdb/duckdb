//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/dictionary_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/once_flag.hpp"
#include "duckdb/common/types/vector.hpp"

#include <variant>

namespace duckdb {

//! The DictionaryEntry holds a child Vector for dictionary-encoded vectors
class DictionaryEntry {
public:
	explicit DictionaryEntry(Vector vector) : data(std::move(vector)) {
	}

public:
	Vector data;
	//! Optional id to uniquely identify re-occurring dictionaries
	string id;
	//! True iff the producer wraps this same entry in every output chunk for its lifetime (stable id, no flat
	//! fall-through), making it a global dictionary. Set only via CreateReusableGlobalDictionary.
	bool global_dictionary = false;
	//! For caching the hashes of a child buffer (mutable: cache is logically const)
	mutable OnceFlag cached_hashes_once;
	mutable unique_ptr<Vector> cached_hashes;
};

//! The DictionaryBuffer holds a selection vector and a reference to a DictionaryEntry
class DictionaryBuffer : public VectorBuffer {
public:
	explicit DictionaryBuffer(const SelectionVector &sel, idx_t sel_count, buffer_ptr<DictionaryEntry> entry_p);
	explicit DictionaryBuffer(buffer_ptr<SelectionData> data, idx_t sel_count, buffer_ptr<DictionaryEntry> entry_p);
	explicit DictionaryBuffer(const SelectionVector &sel, idx_t sel_count);
	explicit DictionaryBuffer(buffer_ptr<SelectionData> data, idx_t sel_count);
	explicit DictionaryBuffer(idx_t count = STANDARD_VECTOR_SIZE);

public:
	idx_t Capacity() const override {
		return Size();
	}
	const SelectionVector &GetSelVector() const {
		return sel_vector;
	}
	SelectionVector &GetSelVector() {
		return sel_vector;
	}
	void SetSelVector(const SelectionVector &vector) {
		this->sel_vector.Initialize(vector);
	}
	bool IsReset() const {
		return std::holds_alternative<Vector>(storage);
	}
	void Reset(const LogicalType &type, const buffer_ptr<VectorBuffer> &flat_buffer);
	Vector &GetChild() {
		return IsReset() ? GetResetVector() : GetEntry().data;
	}
	const Vector &GetChild() const {
		return IsReset() ? GetResetVector() : GetEntry().data;
	}
	void Rebind(buffer_ptr<DictionaryEntry> entry_p, const SelectionVector &sel, idx_t count);
	optional_idx GetDictionarySize() const {
		if (IsReset()) {
			return optional_idx();
		}
		auto &entry = GetDictionaryEntry();
		if (!entry || entry->data.size() == 0) {
			// FIXME: we should be directly returning entry->data.size(), this should not be an optional_idx
			return optional_idx();
		}
		return entry->data.size();
	}
	void SetDictionaryId(string id) {
		GetEntry().id = std::move(id);
	}
	const string &GetDictionaryId() const {
		if (IsReset() || !GetDictionaryEntry()) {
			static const string empty;
			return empty;
		}
		return GetEntry().id;
	}

	DictionaryEntry &GetEntry() {
		return *GetDictionaryEntry();
	}
	const DictionaryEntry &GetEntry() const {
		return *GetDictionaryEntry();
	}
	buffer_ptr<DictionaryEntry> GetEntryPtr() const {
		return IsReset() ? nullptr : GetDictionaryEntry();
	}
	void SetEntry(buffer_ptr<DictionaryEntry> entry_p) {
		storage.emplace<buffer_ptr<DictionaryEntry>>(std::move(entry_p));
	}

public:
	idx_t GetDataSize(const LogicalType &type, idx_t count) const override;
	idx_t GetAllocationSize() const override;
	void ToUnifiedFormat(UnifiedVectorFormat &format) const override;
	buffer_ptr<VectorBuffer> Flatten(const LogicalType &type) const override;
	Value GetValue(const LogicalType &type, idx_t index) const override;
	buffer_ptr<VectorBuffer> SliceWithCache(SelCache &cache, const LogicalType &type, const SelectionVector &sel,
	                                        idx_t count) override;
	bool TrySerialize(Serializer &serializer, const LogicalType &type, bool compressed_serialization) const override;
	static buffer_ptr<VectorBuffer> Deserialize(Deserializer &deserializer, const LogicalType &type, idx_t count);

protected:
	buffer_ptr<VectorBuffer> SliceInternal(const LogicalType &type, idx_t offset, idx_t end) override;
	buffer_ptr<VectorBuffer> SliceInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) override;
	buffer_ptr<VectorBuffer> FlattenSliceInternal(const LogicalType &type, const SelectionVector &sel,
	                                              idx_t count) const override;
	void VerifyInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;

private:
	//! The active dictionary input and its shared metadata; may be null before an entry is attached.
	const buffer_ptr<DictionaryEntry> &GetDictionaryEntry() const {
		return std::get<buffer_ptr<DictionaryEntry>>(storage);
	}
	//! The chunk's empty flat vector while reset, preserving type, capacity, allocator and reusable storage.
	//! Its buffer reference keeps storage available without retaining the previous dictionary input.
	Vector &GetResetVector() {
		return std::get<Vector>(storage);
	}
	const Vector &GetResetVector() const {
		return std::get<Vector>(storage);
	}

private:
	SelectionVector sel_vector;
	//! An input entry or the chunk's typed, empty storage between reset and the next producer.
	std::variant<buffer_ptr<DictionaryEntry>, Vector> storage;
};

class SelectionDataHolder : public AuxiliaryDataHolder {
public:
	explicit SelectionDataHolder(buffer_ptr<SelectionData> selection_data_p)
	    : selection_data(std::move(selection_data_p)) {
	}

private:
	buffer_ptr<SelectionData> selection_data;
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
		return vector.Buffer().Cast<DictionaryBuffer>().GetChild();
	}
	static inline Vector &Child(Vector &vector) {
		VerifyDictionary(vector);
		return vector.BufferMutable().Cast<DictionaryBuffer>().GetChild();
	}
	static inline optional_idx DictionarySize(const Vector &vector) {
		VerifyDictionary(vector);
		const auto &dict_buffer = vector.Buffer().Cast<DictionaryBuffer>();
		return dict_buffer.GetDictionarySize();
	}
	static inline const string &DictionaryId(const Vector &vector) {
		VerifyDictionary(vector);
		const auto &dict_buffer = vector.Buffer().Cast<DictionaryBuffer>();
		return dict_buffer.GetDictionaryId();
	}
	static inline bool CanCacheHashes(const LogicalType &type) {
		return type.InternalType() == PhysicalType::VARCHAR;
	}
	static inline bool CanCacheHashes(const Vector &vector) {
		return DictionarySize(vector).IsValid() && !DictionaryId(vector).empty() && CanCacheHashes(vector.GetType());
	}
	static buffer_ptr<DictionaryEntry> CreateReusableDictionary(const LogicalType &type, const idx_t &size);
	//! Mint a reusable dictionary entry whose lifetime spans the entire producing operator instance
	static buffer_ptr<DictionaryEntry> CreateReusableGlobalDictionary(const LogicalType &type, const idx_t &size);
	//! True iff vector is a DICTIONARY_VECTOR whose entry is a global dictionary
	static inline bool IsGlobalDictionary(const Vector &vector) {
		if (vector.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
			return false;
		}
		auto &buffer = vector.Buffer().Cast<DictionaryBuffer>();
		return !buffer.IsReset() && buffer.GetEntry().global_dictionary;
	}
	static const Vector &GetCachedHashes(const Vector &input);
};

} // namespace duckdb
