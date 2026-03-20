//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/list_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct ListVector {
	static inline const list_entry_t *GetData(const Vector &v) {
		if (v.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			auto &child = DictionaryVector::Child(v);
			return GetData(child);
		}
		return FlatVector::GetData<const list_entry_t>(v);
	}
	static inline list_entry_t *GetData(Vector &v) {
		if (v.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			auto &child = DictionaryVector::Child(v);
			return GetData(child);
		}
		return FlatVector::GetData<list_entry_t>(v);
	}
	//! Gets a reference to the underlying child-vector of a list
	DUCKDB_API static const Vector &GetEntry(const Vector &vector);
	//! Gets a reference to the underlying child-vector of a list
	DUCKDB_API static Vector &GetEntry(Vector &vector);
	//! Gets the total size of the underlying child-vector of a list
	DUCKDB_API static idx_t GetListSize(const Vector &vector);
	//! Sets the total size of the underlying child-vector of a list
	DUCKDB_API static void SetListSize(Vector &vec, idx_t size);
	//! Gets the total capacity of the underlying child-vector of a list
	DUCKDB_API static idx_t GetListCapacity(const Vector &vector);
	//! Sets the total capacity of the underlying child-vector of a list
	DUCKDB_API static void Reserve(Vector &vec, idx_t required_capacity);
	DUCKDB_API static void Append(Vector &target, const Vector &source, idx_t source_size, idx_t source_offset = 0);
	DUCKDB_API static void Append(Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size,
	                              idx_t source_offset = 0);
	DUCKDB_API static void PushBack(Vector &target, const Value &insert);
	//! Returns the child_vector of list starting at offset until offset + count, and its length
	DUCKDB_API static idx_t GetConsecutiveChildList(Vector &list, Vector &result, idx_t offset, idx_t count);
	//! Returns information to only copy a section of a list child vector
	DUCKDB_API static ConsecutiveChildListInfo GetConsecutiveChildListInfo(Vector &list, idx_t offset, idx_t count);
	//! Slice and flatten a child vector to only contain a consecutive subsection of the child entries
	DUCKDB_API static void GetConsecutiveChildSelVector(Vector &list, SelectionVector &sel, idx_t offset, idx_t count);
	//! Share the entry of the other list vector
	DUCKDB_API static void ReferenceEntry(Vector &vector, Vector &other);
	//! Returns the total number of entries in the list
	DUCKDB_API static idx_t GetTotalEntryCount(Vector &list, idx_t count);

private:
	template <class T>
	static T &GetEntryInternal(T &vector);
};

}
