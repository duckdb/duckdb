//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/types/statistics.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.hpp"
#include "common/types/value.hpp"

namespace duckdb {

class Vector;

//! The Statistics object holds statistics relating to a Vector or column
class Statistics {
  public:
	Statistics() : has_stats(false), can_have_null(true) {}
	//! Initialize statistics for a vector with a single value
	Statistics(Value value)
	    : has_stats(true), can_have_null(value.is_null), min(value),
	      max(value) {}

	bool has_stats;
	bool can_have_null;
	Value min;
	Value max;

#ifdef DEBUG
	//! DEBUG FUNCTION ONLY!
	void Verify(Vector &vector);
#else
	//! DEBUG FUNCTION ONLY!
	void Verify(Vector &vector) {}
#endif

	void Reset();

	//===--------------------------------------------------------------------===//
	// Numeric Operations
	//===--------------------------------------------------------------------===//
	// A + B
	static void Add(Statistics &left, Statistics &right, Statistics &result);
	// A - B
	static void Subtract(Statistics &left, Statistics &right,
	                     Statistics &result);
	// A * B
	static void Multiply(Statistics &left, Statistics &right,
	                     Statistics &result);
	// A / B
	static void Divide(Statistics &left, Statistics &right, Statistics &result);
	// A % B
	static void Modulo(Statistics &left, Statistics &right, Statistics &result);
};

} // namespace duckdb
