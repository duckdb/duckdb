//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class Allocator;
class Vector;

//! The VectorCache holds cached vector data.
//! It enables re-using the same memory for different vectors.
class VectorCache {
public:
	//! Instantiate an empty vector cache.
	DUCKDB_API VectorCache();
	//! Instantiate a vector cache with the given type and capacity.
	DUCKDB_API VectorCache(Allocator &allocator, const LogicalType &type, const idx_t capacity = STANDARD_VECTOR_SIZE);

public:
	buffer_ptr<VectorBuffer> buffer;

public:
	void ResetFromCache(Vector &result) const;
	const LogicalType &GetType() const;
};

} // namespace duckdb
