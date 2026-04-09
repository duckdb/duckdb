//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/list_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

class VectorListBuffer : public StandardVectorBuffer {
public:
	explicit VectorListBuffer(Allocator &allocator, idx_t capacity, unique_ptr<Vector> vector,
	                          idx_t child_capacity = STANDARD_VECTOR_SIZE);
	explicit VectorListBuffer(Allocator &allocator, idx_t capacity, const LogicalType &list_type,
	                          idx_t child_capacity = STANDARD_VECTOR_SIZE);
	explicit VectorListBuffer(idx_t capacity, const LogicalType &list_type,
	                          idx_t child_capacity = STANDARD_VECTOR_SIZE);
	explicit VectorListBuffer(data_ptr_t data, const Vector &vector, idx_t child_capacity, idx_t child_size);
	explicit VectorListBuffer(data_ptr_t data, const VectorListBuffer &parent);
	explicit VectorListBuffer(AllocatedData allocated_data, const VectorListBuffer &parent);
	~VectorListBuffer() override;

public:
	Vector &GetChild() {
		return *child;
	}
	const Vector &GetChild() const {
		return *child;
	}
	void Reserve(idx_t to_reserve);

	void Append(const Vector &to_append, idx_t to_append_size, idx_t source_offset = 0);
	void Append(const Vector &to_append, const SelectionVector &sel, idx_t to_append_size, idx_t source_offset = 0);

	void PushBack(const Value &insert);

	idx_t GetSize() const {
		return size;
	}

	idx_t GetCapacity() const {
		return capacity;
	}

	void SetCapacity(idx_t new_capacity);
	void SetSize(idx_t new_size);
	buffer_ptr<VectorBuffer> Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) override;

public:
	idx_t GetAllocationSize() const override;
	Value GetValue(const LogicalType &type, idx_t index) const override;
	void Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;

private:
	//! child vectors used for nested data
	unique_ptr<Vector> child;
	idx_t capacity = 0;
	idx_t size = 0;
};

struct ListVector {
	[[deprecated("Use FlatVector::GetData<list_entry_t> instead")]] static inline const list_entry_t *
	GetData(const Vector &v) {
		if (v.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			throw InternalException("ListVector::GetData called on dictionary vector");
		}
		return FlatVector::GetData<const list_entry_t>(v);
	}
	[[deprecated("Use FlatVector::GetData<list_entry_t> instead")]] static inline list_entry_t *GetData(Vector &v) {
		if (v.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			throw InternalException("ListVector::GetData called on dictionary vector");
		}
		return FlatVector::GetDataMutable<list_entry_t>(v);
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
	//! Returns the total number of entries in the list
	DUCKDB_API static idx_t GetTotalEntryCount(Vector &list, idx_t count);

private:
	template <class T>
	static T &GetEntryInternal(T &vector);
};

} // namespace duckdb
