//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/vector_buffer.hpp"

namespace duckdb {
class Vector;

//! The VectorCache holds cached data for
class VectorCache {
public:
	// Instantiate a vector cache with the given type
	VectorCache(const LogicalType &type);

	//! The type of the vector cache
	LogicalType type;
	//! Owned data
	unique_ptr<data_t[]> owned_data;
	//! Child caches (if any). Used for nested types.
	vector<unique_ptr<VectorCache>> child_caches;
	//! Aux data for the vector (if any)
	buffer_ptr<VectorBuffer> auxiliary;
};

} // namespace duckdb
