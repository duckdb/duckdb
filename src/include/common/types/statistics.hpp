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
	Statistics() { Reset(); }
	Statistics(TypeId type) : type(type) { Reset(); }
	//! Initialize statistics for a vector with a single value
	Statistics(Value value)
	    : has_stats(true), can_have_null(value.is_null), min(value), max(value),
	      type(value.type),
	      maximum_string_length(
	          value.type == TypeId::VARCHAR ? value.str_value.size() : 0) {}

	//! Whether or not statistics are available for the given column
	bool has_stats;
	//! True if the vector can potentially contain NULL values
	bool can_have_null;
	//! The minimum value of the column [numeric only]
	Value min;
	//! The maximum value of the column [numeric only]
	Value max;
	//! The maximum string length of a character column [VARCHAR only]
	uint64_t maximum_string_length;
	//! The type of the vector the statistics are for
	TypeId type;

#ifdef DEBUG
	//! Verify that the statistics hold for a given vector. DEBUG FUNCTION ONLY!
	void Verify(Vector &vector);
#else
	//! Verify that the statistics hold for a given vector. DEBUG FUNCTION ONLY!
	void Verify(Vector &vector) {}
#endif

	//! Update the statistics with a new vector that is appended to the dataset
	void Update(Vector &vector);
	//! Reset the statistics
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
