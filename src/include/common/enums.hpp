//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

#include <cstdlib>
#include <math.h>

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
	// binary arithmetic operator boundary, used for quick comparisons
	BINOP_BOUNDARY_END = OPERATOR_MOD,
	// explicitly cast left as right (right is integer in ValueType enum)
	OPERATOR_CAST = 7,
	// logical not operator
	OPERATOR_NOT = 8,
	// is null operator
	OPERATOR_IS_NULL = 9,
	// is not null operator
	OPERATOR_IS_NOT_NULL = 10,

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
	// IN operator [left IN (right1, right2, ...)]
	COMPARE_IN = 33,
	// NOT IN operator [left NOT IN (right1, right2, ...)]
	COMPARE_NOT_IN = 34,
	// IS DISTINCT FROM operator
	COMPARE_DISTINCT_FROM = 35,
	// compare final boundary

	COMPARE_BETWEEN = 36,
	COMPARE_NOT_BETWEEN = 37,
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

//===--------------------------------------------------------------------===//
// Constraint Types
//===--------------------------------------------------------------------===//
enum class ConstraintType : uint8_t {
	INVALID = 0,     // invalid constraint type
	NOT_NULL = 1,    // NOT NULL constraint
	CHECK = 2,       // CHECK constraint
	PRIMARY_KEY = 3, // PRIMARY KEY constraint
	UNIQUE = 4,      // UNIQUE constraint
	FOREIGN_KEY = 5, // FOREIGN KEY constraint
	DUMMY = 6        // Dummy constraint used by parser
};

//===--------------------------------------------------------------------===//
// Catalog Types
//===--------------------------------------------------------------------===//
enum class CatalogType : uint8_t {
	INVALID = 0,
	TABLE = 1,
	SCHEMA = 2,
	TABLE_FUNCTION = 3,
	SCALAR_FUNCTION = 4,
	VIEW = 5,
	INDEX = 6,
	UPDATED_ENTRY = 10,
	DELETED_ENTRY = 11,
	PREPARED_STATEMENT = 12
};

//===--------------------------------------------------------------------===//
// Subquery Types
//===--------------------------------------------------------------------===//
enum class SubqueryType : uint8_t {
	INVALID = 0,
	SCALAR = 1,     // Regular scalar subquery
	EXISTS = 2,     // EXISTS (SELECT...)
	NOT_EXISTS = 3, // NOT EXISTS(SELECT...)
	ANY = 4,        // x = ANY(SELECT...) OR x IN (SELECT...)
};

//===--------------------------------------------------------------------===//
// Statement Types
//===--------------------------------------------------------------------===//
enum class StatementType : uint8_t {
	INVALID,      // invalid statement type
	SELECT,       // select statement type
	INSERT,       // insert statement type
	UPDATE,       // update statement type
	DELETE,       // delete statement type
	DEALLOCATE,   // de-allocate statement type
	PREPARE,      // prepare statement type
	EXECUTE,      // execute statement type
	ALTER,        // alter statement type
	TRANSACTION,  // transaction statement type,
	COPY,         // copy type
	ANALYZE,      // analyze type
	VARIABLE_SET, // variable set statement type
	CREATE_FUNC,  // create func statement type
	EXPLAIN,      // explain statement type

	// -----------------------------
	// Create Types
	// -----------------------------
	CREATE_TABLE,  // create table statement type
	CREATE_SCHEMA, // create schema statement type
	CREATE_INDEX,  // create index statement type
	CREATE_VIEW,   // create view statement type

	// -----------------------------
	// Drop Types
	// -----------------------------
	DROP_TABLE,  // drop table statement type
	DROP_SCHEMA, // drop table statement type
	DROP_INDEX,  // drop index statement type
	DROP_VIEW    // drop view statement type
};

//===--------------------------------------------------------------------===//
// Table Reference Types
//===--------------------------------------------------------------------===//
enum class TableReferenceType : uint8_t {
	INVALID = 0,       // invalid table reference type
	BASE_TABLE = 1,    // base table reference
	SUBQUERY = 2,      // output of a subquery
	JOIN = 3,          // output of join
	CROSS_PRODUCT = 4, // out of cartesian product
	TABLE_FUNCTION = 5 // table producing function
};

//===--------------------------------------------------------------------===//
// Join Types
//===--------------------------------------------------------------------===//
enum class JoinType : uint8_t {
	INVALID = 0, // invalid join type
	LEFT = 1,    // left
	RIGHT = 2,   // right
	INNER = 3,   // inner
	OUTER = 4,   // outer
	SEMI = 5,    // SEMI join returns left side row ONLY if it has a join partner, no duplicates
	ANTI = 6,    // ANTI join returns left side row ONLY if it has NO join partner, no duplicates
	MARK = 7,    // MARK join returns marker indicating whether or not there is a join partner (true), there is no join
	             // partner (false)
	SINGLE = 8   // SINGLE join is like LEFT OUTER JOIN, BUT returns at most one join partner per entry on the LEFT side
	             // (and NULL if no partner is found)
};

//===--------------------------------------------------------------------===//
// Index Types
//===--------------------------------------------------------------------===//

enum class IndexType {
	ORDER_INDEX = 1, // Order Index
	ART = 2        // Adaptive Radix Tree
};

//===--------------------------------------------------------------------===//
// ORDER BY Clause Types
//===--------------------------------------------------------------------===//
enum class OrderType : uint8_t { INVALID = 0, ASCENDING = 1, DESCENDING = 2 };

enum class SetOperationType : uint8_t { NONE = 0, UNION = 1, EXCEPT = 2, INTERSECT = 3 };

//===--------------------------------------------------------------------===//
// Logical Operator Types
//===--------------------------------------------------------------------===//
enum class LogicalOperatorType : uint8_t {
	INVALID,
	PROJECTION,
	FILTER,
	AGGREGATE_AND_GROUP_BY,
	WINDOW,
	LIMIT,
	ORDER_BY,
	COPY,
	DISTINCT,
	INDEX_SCAN,
	// -----------------------------
	// Data sources
	// -----------------------------
	GET,
	CHUNK_GET,
	DELIM_GET,
	TABLE_FUNCTION,
	SUBQUERY,
	EMPTY_RESULT,
	// -----------------------------
	// Joins
	// -----------------------------
	JOIN,
	DELIM_JOIN,
	COMPARISON_JOIN,
	ANY_JOIN,
	CROSS_PRODUCT,
	// -----------------------------
	// SetOps
	// -----------------------------
	UNION,
	EXCEPT,
	INTERSECT,

	// -----------------------------
	// Updates
	// -----------------------------
	INSERT,
	DELETE,
	UPDATE,
	CREATE_TABLE,
	CREATE_INDEX,

	// -----------------------------
	// Explain
	// -----------------------------
	EXPLAIN,

	// -----------------------------
	// Helpers
	// -----------------------------
	PRUNE_COLUMNS,
	PREPARE,
	EXECUTE
};

//===--------------------------------------------------------------------===//
// Physical Operator Types
//===--------------------------------------------------------------------===//
enum class PhysicalOperatorType : uint8_t {
	INVALID,
	LEAF,
	ORDER_BY,
	LIMIT,
	AGGREGATE,
	WINDOW,
	DISTINCT,
	HASH_GROUP_BY,
	SORT_GROUP_BY,
	FILTER,
	PROJECTION,
	BASE_GROUP_BY,
	COPY,
	TABLE_FUNCTION,
	// -----------------------------
	// Scans
	// -----------------------------
	DUMMY_SCAN,
	SEQ_SCAN,
	INDEX_SCAN,
	CHUNK_SCAN,
	DELIM_SCAN,
	EXTERNAL_FILE_SCAN,
	QUERY_DERIVED_SCAN,
	// -----------------------------
	// Joins
	// -----------------------------
	BLOCKWISE_NL_JOIN,
	NESTED_LOOP_JOIN,
	HASH_JOIN,
	CROSS_PRODUCT,
	PIECEWISE_MERGE_JOIN,
	DELIM_JOIN,

	// -----------------------------
	// SetOps
	// -----------------------------
	UNION,

	// -----------------------------
	// Updates
	// -----------------------------
	INSERT,
	INSERT_SELECT,
	DELETE,
	UPDATE,
	EXPORT_EXTERNAL_FILE,
	CREATE,
	CREATE_INDEX,
	// -----------------------------
	// Helpers
	// -----------------------------
	PRUNE_COLUMNS,
	EXPLAIN,
	EMPTY_RESULT,
	EXECUTE
};

//===--------------------------------------------------------------------===//
// Match Order
//===--------------------------------------------------------------------===//
enum class MatchOrder : uint8_t { ARBITRARY, DEPTH_FIRST };
//===--------------------------------------------------------------------===//
// Child Match Policy
//===--------------------------------------------------------------------===//
enum class ChildPolicy : uint8_t { ALWAYS_MATCH, ANY, LEAF, SOME, UNORDERED, ORDERED };

//===--------------------------------------------------------------------===//
// External File Format Types
//===--------------------------------------------------------------------===//
enum class ExternalFileFormat : uint8_t { INVALID, CSV };

//===--------------------------------------------------------------------===//
// Transaction Operation Types
//===--------------------------------------------------------------------===//
enum class TransactionType : uint8_t { INVALID, BEGIN_TRANSACTION, COMMIT, ROLLBACK };

//===--------------------------------------------------------------------===//
// String <-> Enum conversion
//===--------------------------------------------------------------------===//
string JoinTypeToString(JoinType type);
IndexType StringToIndexType(const string &str);

string LogicalOperatorToString(LogicalOperatorType type);
string PhysicalOperatorToString(PhysicalOperatorType type);
string ExpressionTypeToString(ExpressionType type);
string ExpressionTypeToOperator(ExpressionType type);

ExternalFileFormat StringToExternalFileFormat(const string &str);

} // namespace duckdb
