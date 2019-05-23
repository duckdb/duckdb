//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums/expression_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Predicate Expression Operation Types
//===--------------------------------------------------------------------===//
enum class ExpressionType : uint8_t {
	INVALID = 0,

	// -----------------------------
	// Arithmetic Operators
	// -----------------------------
	// left + right (both must be number. implicitly casted)
	OPERATOR_ADD = 1,
	// start of binary add
	BINOP_BOUNDARY_START = OPERATOR_ADD,
	// left - right (both must be number. implicitly casted)
	OPERATOR_SUBTRACT = 2,
	// left * right (both must be number. implicitly casted)
	OPERATOR_MULTIPLY = 3,
	// left / right (both must be number. implicitly casted)
	OPERATOR_DIVIDE = 4,
	// left || right (both must be char/varchar)
	OPERATOR_CONCAT = 5,
	// left % right (both must be integer)
	OPERATOR_MOD = 6,
	// left << right (both must be integer)
	OPERATOR_LSHIFT = 7,
	// left >> right (both must be integer)
	OPERATOR_RSHIFT = 8,
	// left & right (both must be integer)
	OPERATOR_BITWISE_AND = 9,
	// left & right (both must be integer)
	OPERATOR_BITWISE_OR = 10,
	// left & right (both must be integer)
	OPERATOR_BITWISE_XOR = 11,
	// binary arithmetic operator boundary, used for quick comparisons
	BINOP_BOUNDARY_END = OPERATOR_BITWISE_XOR,
	// explicitly cast left as right (right is integer in ValueType enum)
	OPERATOR_CAST = 12,
	// logical not operator
	OPERATOR_NOT = 13,
	// is null operator
	OPERATOR_IS_NULL = 14,
	// is not null operator
	OPERATOR_IS_NOT_NULL = 15,

	// -----------------------------
	// Comparison Operators
	// -----------------------------
	// equal operator between left and right
	COMPARE_EQUAL = 25,
	// compare initial boundary
	COMPARE_BOUNDARY_START = COMPARE_EQUAL,
	// inequal operator between left and right
	COMPARE_NOTEQUAL = 26,
	// less than operator between left and right
	COMPARE_LESSTHAN = 27,
	// greater than operator between left and right
	COMPARE_GREATERTHAN = 28,
	// less than equal operator between left and right
	COMPARE_LESSTHANOREQUALTO = 29,
	// greater than equal operator between left and right
	COMPARE_GREATERTHANOREQUALTO = 30,
	// LIKE operator (left LIKE right). Both children must be string.
	COMPARE_LIKE = 31,
	// NOT LIKE operator (left NOT LIKE right). Both children must be string.
	COMPARE_NOTLIKE = 32,
	// SIMILAR TO operator (left SIMILAR TO right). Both children must be string.
	COMPARE_SIMILAR = 33,
	// NOT SIMILAR TO operator (left NOT SIMILAR TO right). Both children must be string.
	COMPARE_NOTSIMILAR = 34,
	// IN operator [left IN (right1, right2, ...)]
	COMPARE_IN = 35,
	// NOT IN operator [left NOT IN (right1, right2, ...)]
	COMPARE_NOT_IN = 36,
	// IS DISTINCT FROM operator
	COMPARE_DISTINCT_FROM = 37,
	// compare final boundary

	COMPARE_BETWEEN = 38,
	COMPARE_NOT_BETWEEN = 39,
	COMPARE_BOUNDARY_END = COMPARE_NOT_BETWEEN,

	// -----------------------------
	// Conjunction Operators
	// -----------------------------
	CONJUNCTION_AND = 50,
	CONJUNCTION_OR = 51,

	// -----------------------------
	// Values
	// -----------------------------
	VALUE_CONSTANT = 75,
	VALUE_PARAMETER = 76,
	VALUE_TUPLE = 77,
	VALUE_TUPLE_ADDRESS = 78,
	VALUE_NULL = 79,
	VALUE_VECTOR = 80,
	VALUE_SCALAR = 81,
	VALUE_DEFAULT = 82,

	// -----------------------------
	// Aggregates
	// -----------------------------
	AGGREGATE_COUNT = 100,
	AGGREGATE_COUNT_STAR = 101,
	AGGREGATE_COUNT_DISTINCT = 102,
	AGGREGATE_SUM = 103,
	AGGREGATE_SUM_DISTINCT = 104,
	AGGREGATE_MIN = 105,
	AGGREGATE_MAX = 106,
	AGGREGATE_AVG = 107,
	AGGREGATE_FIRST = 108,
	AGGREGATE_STDDEV_SAMP = 109,

	WINDOW_SUM = 115,
	WINDOW_COUNT_STAR = 116,
	WINDOW_MIN = 117,
	WINDOW_MAX = 118,
	WINDOW_AVG = 119,

	WINDOW_RANK = 120,
	WINDOW_RANK_DENSE = 121,
	WINDOW_NTILE = 122,
	WINDOW_PERCENT_RANK = 123,
	WINDOW_CUME_DIST = 124,
	WINDOW_ROW_NUMBER = 125,

	WINDOW_FIRST_VALUE = 130,
	WINDOW_LAST_VALUE = 131,
	WINDOW_LEAD = 132,
	WINDOW_LAG = 133,

	// -----------------------------
	// Functions
	// -----------------------------
	FUNCTION = 140,
	BOUND_FUNCTION = 141,

	// -----------------------------
	// Operators
	// -----------------------------
	OPERATOR_CASE_EXPR = 150,
	OPERATOR_NULLIF = 151,
	OPERATOR_COALESCE = 152,

	// -----------------------------
	// Subquery IN/EXISTS
	// -----------------------------
	SUBQUERY = 175,

	// -----------------------------
	// Parser
	// -----------------------------
	STAR = 200,
	PLACEHOLDER = 201,
	COLUMN_REF = 202,
	FUNCTION_REF = 203,
	TABLE_REF = 204,

	// -----------------------------
	// Miscellaneous
	// -----------------------------
	CAST = 225,
	COMMON_SUBEXPRESSION = 226,
	BOUND_REF = 227,
	BOUND_COLUMN_REF = 228
};

//===--------------------------------------------------------------------===//
// Expression Class
//===--------------------------------------------------------------------===//
enum class ExpressionClass : uint8_t {
	INVALID = 0,
	//===--------------------------------------------------------------------===//
	// Parsed Expressions
	//===--------------------------------------------------------------------===//
	AGGREGATE = 1,
	CASE = 2,
	CAST = 3,
	COLUMN_REF = 4,
	COMPARISON = 5,
	CONJUNCTION = 6,
	CONSTANT = 7,
	DEFAULT = 8,
	FUNCTION = 9,
	OPERATOR = 10,
	STAR = 11,
	SUBQUERY = 12,
	WINDOW = 13,
	PARAMETER = 14,
	//===--------------------------------------------------------------------===//
	// Bound Expressions
	//===--------------------------------------------------------------------===//
	BOUND_AGGREGATE = 25,
	BOUND_CASE = 26,
	BOUND_CAST = 27,
	BOUND_COLUMN_REF = 28,
	BOUND_COMPARISON = 29,
	BOUND_CONJUNCTION = 30,
	BOUND_CONSTANT = 31,
	BOUND_DEFAULT = 32,
	BOUND_FUNCTION = 33,
	BOUND_OPERATOR = 34,
	BOUND_PARAMETER = 35,
	BOUND_REF = 36,
	BOUND_SUBQUERY = 37,
	BOUND_WINDOW = 38,
	//===--------------------------------------------------------------------===//
	// Miscellaneous
	//===--------------------------------------------------------------------===//
	BOUND_EXPRESSION = 50,
	COMMON_SUBEXPRESSION = 51
};

string ExpressionTypeToString(ExpressionType type);
string ExpressionTypeToOperator(ExpressionType type);

//! Negate a comparison expression, turning e.g. = into !=, or < into >=
ExpressionType NegateComparisionExpression(ExpressionType type);
//! Flip a comparison expression, turning e.g. < into >, or = into =
ExpressionType FlipComparisionExpression(ExpressionType type);

} // namespace duckdb
