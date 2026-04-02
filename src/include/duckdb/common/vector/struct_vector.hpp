//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/struct_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

class VectorStructBuffer : public VectorBuffer {
public:
	VectorStructBuffer();
	explicit VectorStructBuffer(const LogicalType &struct_type, idx_t capacity = STANDARD_VECTOR_SIZE);
	VectorStructBuffer(Vector &other, const SelectionVector &sel, idx_t count);
	~VectorStructBuffer() override;

public:
	const vector<Vector> &GetChildren() const {
		return children;
	}
	vector<Vector> &GetChildren() {
		return children;
	}

private:
	//! child vectors used for nested data
	vector<Vector> children;
};

struct StructVector {
	DUCKDB_API static const vector<Vector> &GetEntries(const Vector &vector);
	DUCKDB_API static vector<Vector> &GetEntries(Vector &vector);
};

} // namespace duckdb
