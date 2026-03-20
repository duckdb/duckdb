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

}
