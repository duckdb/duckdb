//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/array_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

class VectorArrayBuffer : public VectorBuffer {
public:
	explicit VectorArrayBuffer(unique_ptr<Vector> child_vector, idx_t array_size, idx_t initial_capacity);
	explicit VectorArrayBuffer(const LogicalType &array, idx_t initial = STANDARD_VECTOR_SIZE);
	~VectorArrayBuffer() override;

public:
	ValidityMask &GetValidityMask() override {
		return validity;
	}
	Vector &GetChild();
	idx_t GetArraySize() const;
	idx_t GetChildSize() const;
	void SetVectorType(VectorType vector_type) override;
	buffer_ptr<VectorBuffer> Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) override;

public:
	idx_t GetAllocationSize() const override;
	void Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;

private:
	ValidityMask validity;
	unique_ptr<Vector> child;
	// The size of each array in this buffer
	idx_t array_size = 0;
	// How many arrays are currently stored in this buffer
	// The child vector has size (array_size * size)
	idx_t size = 0;
};

struct ArrayVector {
	//! Gets a reference to the underlying child-vector of an array
	DUCKDB_API static const Vector &GetEntry(const Vector &vector);
	//! Gets a reference to the underlying child-vector of an array
	DUCKDB_API static Vector &GetEntry(Vector &vector);
	//! Gets the total size of the underlying child-vector of an array
	DUCKDB_API static idx_t GetTotalSize(const Vector &vector);

private:
	template <class T>
	static T &GetEntryInternal(T &vector);
};

} // namespace duckdb
