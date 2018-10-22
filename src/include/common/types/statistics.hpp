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
	Statistics() {
		Reset();
	}
	Statistics(TypeId type) : type(type) {
		Reset();
	}
	//! Initialize statistics for a vector with a single value
	Statistics(Value value);

	bool CanHaveNull() {
		return !has_stats || can_have_null;
	}

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
	//! The maximum amount of elements that can be found in this vector
	size_t maximum_count;

#ifdef DEBUG
	//! Verify that the statistics hold for a given vector. DEBUG FUNCTION ONLY!
	void Verify(Vector &vector);
#else
	//! Verify that the statistics hold for a given vector. DEBUG FUNCTION ONLY!
	void Verify(Vector &vector) {
	}
#endif

	//! Update the statistics with a new vector that is appended to the dataset
	void Update(Vector &vector);
	//! Reset the statistics
	void Reset();

	//! Returns whether or not the statistics could potentially overflow a type
	bool FitsInType(TypeId type);
	//! Returns the minimal type that guarantees a value from not overflowing
	TypeId MinimalType();

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

	//===--------------------------------------------------------------------===//
	// Aggregates
	//===--------------------------------------------------------------------===//
	// SUM(A)
	static void Sum(Statistics &source, Statistics &result);
	// COUNT(A)
	static void Count(Statistics &source, Statistics &result);
	// MAX(A)
	static void Max(Statistics &source, Statistics &result);
	// MIN(A)
	static void Min(Statistics &source, Statistics &result);

	//! Cast statistics from one type to another
	static void Cast(Statistics &source, Statistics &result);
};

} // namespace duckdb
