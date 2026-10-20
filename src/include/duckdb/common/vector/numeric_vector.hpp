//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/numeric_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

//! Helpers for NUMERIC vectors, whose values are either typed (a shredded vector holding one exact representation) or
//! canonical blobs
struct NumericVector {
	//! The typed representation of a NUMERIC vector: the shredded child, or a decoded constant; false when the vector
	//! holds canonical blobs that are not a constant
	DUCKDB_API static bool TryGetTyped(const Vector &vector, unique_ptr<Vector> &typed);
	//! Both operands of a comparison in one exact representation that holds the values of both; false when either side
	//! has no typed representation
	DUCKDB_API static bool TryNativePair(const Vector &left, const Vector &right, unique_ptr<Vector> &left_typed,
	                                     unique_ptr<Vector> &right_typed);
};

} // namespace duckdb
