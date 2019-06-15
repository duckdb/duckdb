//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types/statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/value.hpp"
#include "common/types/vector.hpp"
#include "planner/expression.hpp"

namespace duckdb {

class ExpressionStatistics;

//! The ExpressionStatistics object holds statistics relating to a specific
//! Expression, and can be propagated throughout a chain of expressions
class ExpressionStatistics {
public:
	ExpressionStatistics(Expression &expression) : expression(expression) {
		Reset();
	}
	//! Copy assignment
	ExpressionStatistics &operator=(const ExpressionStatistics &other);

	bool CanHaveNull() {
		return !has_stats || can_have_null;
	}

	//! Set the statistics to relate to a single value
	void SetFromValue(Value value);

	//! The expression which this statistics relate to
	Expression &expression;
	//! Whether or not statistics are available for the given column
	bool has_stats;
	//! True if the vector can potentially contain NULL values
	bool can_have_null;
	//! The minimum value of the column [numeric only]
	Value min;
	//! The maximum value of the column [numeric only]
	Value max;
	//! The maximum string length of a character column [VARCHAR only]
	index_t maximum_string_length;

	//! Verify that the statistics hold for a given vector. DEBUG FUNCTION ONLY!
	void Verify(Vector &vector);

	virtual string ToString() const;
	void Print();

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
	static void Add(ExpressionStatistics &left, ExpressionStatistics &right, ExpressionStatistics &result);
	// A - B
	static void Subtract(ExpressionStatistics &left, ExpressionStatistics &right, ExpressionStatistics &result);
	// A * B
	static void Multiply(ExpressionStatistics &left, ExpressionStatistics &right, ExpressionStatistics &result);
	// A / B
	static void Divide(ExpressionStatistics &left, ExpressionStatistics &right, ExpressionStatistics &result);
	// A % B
	static void Modulo(ExpressionStatistics &left, ExpressionStatistics &right, ExpressionStatistics &result);
	//===--------------------------------------------------------------------===//
	// String Operations
	//===--------------------------------------------------------------------===//
	// A || B
	static void Concat(ExpressionStatistics &left, ExpressionStatistics &right, ExpressionStatistics &result);

	//===--------------------------------------------------------------------===//
	// Aggregates
	//===--------------------------------------------------------------------===//
	// SUM(A)
	static void Sum(ExpressionStatistics &source, ExpressionStatistics &result);
	// COUNT(A)
	static void Count(ExpressionStatistics &source, ExpressionStatistics &result);
	// MAX(A)
	static void Max(ExpressionStatistics &source, ExpressionStatistics &result);
	// MIN(A)
	static void Min(ExpressionStatistics &source, ExpressionStatistics &result);

	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	//! Cast statistics from one type to another
	static void Cast(ExpressionStatistics &source, ExpressionStatistics &result);
	//! Case operator, we don't need the CHECK column but only the LEFT and
	//! RIGHT side to combine statistics
	static void Case(ExpressionStatistics &A, ExpressionStatistics &B, ExpressionStatistics &result);
};

} // namespace duckdb
