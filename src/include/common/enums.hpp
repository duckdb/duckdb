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
// Internal Types
//===--------------------------------------------------------------------===//
enum class TypeId : uint8_t {
	INVALID = 0,
	PARAMETER_OFFSET = 1,
	BOOLEAN = 2,
	TINYINT = 3,
	SMALLINT = 4,
	INTEGER = 5,
	BIGINT = 6,
	POINTER = 7,
	DATE = 8,
	TIMESTAMP = 9,
	DECIMAL = 10,
	VARCHAR = 11,
	VARBINARY = 12,
	ARRAY = 13,
	UDT = 14
};

//===--------------------------------------------------------------------===//
// Predicate Expression Operation Types
//===--------------------------------------------------------------------===//
enum class ExpressionType : uint8_t {
	INVALID = 0,

	// -----------------------------
	// Arithmetic Operators
	// Implicit Numeric Casting: Trying to implement SQL-92.
	// Implicit Character Casting: Trying to implement SQL-92, but not easy...
	// Anyway, use explicit OPERATOR_CAST if you could.
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
	// exists test.
	OPERATOR_EXISTS = 11,
	// not exists test
	OPERATOR_NOT_EXISTS = 12,

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
	ROW_SUBQUERY = 175,
	SELECT_SUBQUERY = 176,

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
	AGGREGATE = 1,
	CASE = 3,
	CAST = 4,
	COLUMN_REF = 5,
	COMPARISON = 6,
	CONJUNCTION = 7,
	CONSTANT = 8,
	DEFAULT = 9,
	FUNCTION = 10,
	OPERATOR = 12,
	STAR = 13,
	SUBQUERY = 14,
	WINDOW = 15,
	COMMON_SUBEXPRESSION = 16,
	BOUND_REF = 17,
	BOUND_COLUMN_REF = 18,
	BOUND_FUNCTION = 19,
	BOUND_SUBQUERY = 20
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
	DELETED_ENTRY = 11
};

//===--------------------------------------------------------------------===//
// Subquery Types
//===--------------------------------------------------------------------===//
enum class SubqueryType : uint8_t { INVALID = 0, DEFAULT = 1, EXISTS = 2, IN = 3 };

//===--------------------------------------------------------------------===//
// Statement Types
//===--------------------------------------------------------------------===//
enum class StatementType : uint8_t {
	INVALID,      // invalid statement type
	SELECT,       // select statement type
	INSERT,       // insert statement type
	UPDATE,       // update statement type
	DELETE,       // delete statement type
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

	// -----------------------------
	// Drop Types
	// -----------------------------
	DROP_TABLE,  // drop table statement type
	DROP_SCHEMA, // drop table statement type
	DROP_INDEX   // create index statement type
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
	SEMI = 5,    // IN+Subquery is SEMI
	ANTI = 6     // Opposite of SEMI JOIN
};

//===--------------------------------------------------------------------===//
// Index Types
//===--------------------------------------------------------------------===//

enum class IndexType {
	INVALID = 0,     // invalid index type
	ORDER_INDEX = 1, // Order Index
	BTREE = 2        // B+-Tree
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
	LEAF,
	GET,
	EXTERNAL_FILE_GET,
	QUERY_DERIVED_GET,
	PROJECTION,
	FILTER,
	AGGREGATE_AND_GROUP_BY,
	WINDOW,
	LIMIT,
	ORDER_BY,
	COPY,
	SUBQUERY,
	TABLE_FUNCTION,
	// -----------------------------
	// Joins
	// -----------------------------
	JOIN,
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
	INSERT_SELECT,
	DELETE,
	UPDATE,
	EXPORT_EXTERNAL_FILE,
	CREATE,
	CREATE_INDEX,
	ALTER,

	// -----------------------------
	// Explain
	// -----------------------------
	EXPLAIN,

	// -----------------------------
	// Helpers
	// -----------------------------
	PRUNE_COLUMNS
};

//===--------------------------------------------------------------------===//
// Physical Operator Types
//===--------------------------------------------------------------------===//
enum class PhysicalOperatorType : uint8_t {
	INVALID,
	LEAF,
	DUMMY_SCAN,
	SEQ_SCAN,
	INDEX_SCAN,
	EXTERNAL_FILE_SCAN,
	QUERY_DERIVED_SCAN,
	ORDER_BY,
	LIMIT,
	AGGREGATE,
	WINDOW,
	HASH_GROUP_BY,
	SORT_GROUP_BY,
	FILTER,
	PROJECTION,
	BASE_GROUP_BY,
	COPY,
	TABLE_FUNCTION,
	// -----------------------------
	// Joins
	// -----------------------------
	NESTED_LOOP_JOIN,
	HASH_JOIN,
	CROSS_PRODUCT,
	PIECEWISE_MERGE_JOIN,

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
	EMPTY_RESULT
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
string TypeIdToString(TypeId type);
string JoinTypeToString(JoinType type);
IndexType StringToIndexType(const string &str);

string LogicalOperatorToString(LogicalOperatorType type);
string PhysicalOperatorToString(PhysicalOperatorType type);
string ExpressionTypeToString(ExpressionType type);
string ExpressionTypeToOperator(ExpressionType type);

ExternalFileFormat StringToExternalFileFormat(const string &str);

} // namespace duckdb
