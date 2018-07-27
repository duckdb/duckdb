
#pragma once

#include <string>

namespace duckdb {

typedef size_t oid_t;

//===--------------------------------------------------------------------===//
// SQL Value Types
//===--------------------------------------------------------------------===//
enum class TypeId {
	INVALID = 0,
	PARAMETER_OFFSET,
	BOOLEAN,
	TINYINT,
	SMALLINT,
	INTEGER,
	BIGINT,
	DECIMAL,
	POINTER,
	TIMESTAMP,
	DATE,
	VARCHAR,
	VARBINARY,
	ARRAY,
	UDT
};

//===--------------------------------------------------------------------===//
// Statement Types
//===--------------------------------------------------------------------===//
enum class StatementType {
	INVALID = 0,       // invalid statement type
	SELECT = 1,        // select statement type
	INSERT = 3,        // insert statement type
	UPDATE = 4,        // update statement type
	DELETE = 5,        // delete statement type
	CREATE = 6,        // create statement type
	DROP = 7,          // drop statement type
	PREPARE = 8,       // prepare statement type
	EXECUTE = 9,       // execute statement type
	RENAME = 11,       // rename statement type
	ALTER = 12,        // alter statement type
	TRANSACTION = 13,  // transaction statement type,
	COPY = 14,         // copy type
	ANALYZE = 15,      // analyze type
	VARIABLE_SET = 16, // variable set statement type
	CREATE_FUNC = 17,  // create func statement type
	EXPLAIN = 18       // explain statement type
};

//===--------------------------------------------------------------------===//
// Predicate Expression Operation Types
//===--------------------------------------------------------------------===//
enum class ExpressionType {
	INVALID = 0,

	// -----------------------------
	// Arithmetic Operators
	// Implicit Numeric Casting: Trying to implement SQL-92.
	// Implicit Character Casting: Trying to implement SQL-92, but not easy...
	// Anyway, use explicit OPERATOR_CAST if you could.
	// -----------------------------

	// left + right (both must be number. implicitly casted)
	OPERATOR_PLUS = 1,
	// left - right (both must be number. implicitly casted)
	OPERATOR_MINUS = 2,
	// left * right (both must be number. implicitly casted)
	OPERATOR_MULTIPLY = 3,
	// left / right (both must be number. implicitly casted)
	OPERATOR_DIVIDE = 4,
	// left || right (both must be char/varchar)
	OPERATOR_CONCAT = 5,
	// left % right (both must be integer)
	OPERATOR_MOD = 6,
	// explicitly cast left as right (right is integer in ValueType enum)
	OPERATOR_CAST = 7,
	// logical not operator
	OPERATOR_NOT = 8,
	// is null operator
	OPERATOR_IS_NULL = 21,
	// is not null operator
	OPERATOR_IS_NOT_NULL = 22,
	// exists test.
	OPERATOR_EXISTS = 18,
	OPERATOR_UNARY_MINUS = 60,

	// -----------------------------
	// Comparison Operators
	// -----------------------------
	// equal operator between left and right
	COMPARE_EQUAL = 10,
	// inequal operator between left and right
	COMPARE_NOTEQUAL = 11,
	// less than operator between left and right
	COMPARE_LESSTHAN = 12,
	// greater than operator between left and right
	COMPARE_GREATERTHAN = 13,
	// less than equal operator between left and right
	COMPARE_LESSTHANOREQUALTO = 14,
	// greater than equal operator between left and right
	COMPARE_GREATERTHANOREQUALTO = 15,
	// LIKE operator (left LIKE right). Both children must be string.
	COMPARE_LIKE = 16,
	// NOT LIKE operator (left NOT LIKE right). Both children must be string.
	COMPARE_NOTLIKE = 17,
	// IN operator [left IN (right1, right2, ...)]
	COMPARE_IN = 19,
	// IS DISTINCT FROM operator
	COMPARE_DISTINCT_FROM = 20,

	// -----------------------------
	// Conjunction Operators
	// -----------------------------
	CONJUNCTION_AND = 30,
	CONJUNCTION_OR = 31,

	// -----------------------------
	// Values
	// -----------------------------
	VALUE_CONSTANT = 40,
	VALUE_PARAMETER = 41,
	VALUE_TUPLE = 42,
	VALUE_TUPLE_ADDRESS = 43,
	VALUE_NULL = 44,
	VALUE_VECTOR = 45,
	VALUE_SCALAR = 46,

	// -----------------------------
	// Aggregates
	// -----------------------------
	AGGREGATE_COUNT = 50,
	AGGREGATE_COUNT_STAR = 51,
	AGGREGATE_SUM = 52,
	AGGREGATE_MIN = 53,
	AGGREGATE_MAX = 54,
	AGGREGATE_AVG = 55,

	// -----------------------------
	// Functions
	// -----------------------------
	FUNCTION = 100,

	// -----------------------------
	// Internals added for Elastic
	// -----------------------------
	HASH_RANGE = 200,

	// -----------------------------
	// Operators
	// -----------------------------
	OPERATOR_CASE_EXPR = 302,
	OPERATOR_NULLIF = 304,
	OPERATOR_COALESCE = 305,

	// -----------------------------
	// Subquery IN/EXISTS
	// -----------------------------
	ROW_SUBQUERY = 400,
	SELECT_SUBQUERY = 401,

	// -----------------------------
	// Parser
	// -----------------------------
	STAR = 500,
	PLACEHOLDER = 501,
	COLUMN_REF = 502,
	FUNCTION_REF = 503,
	TABLE_REF = 504,

	// -----------------------------
	// Miscellaneous
	// -----------------------------
	CAST = 600
};

//===--------------------------------------------------------------------===//
// Table Reference Types
//===--------------------------------------------------------------------===//
enum class TableReferenceType {
	INVALID = 0,      // invalid table reference type
	BASE_TABLE = 1,   // base table reference
	SUBQUERY = 2,     // output of a subquery
	JOIN = 3,         // output of join
	CROSS_PRODUCT = 4 // out of cartesian product
};

//===--------------------------------------------------------------------===//
// Join Types
//===--------------------------------------------------------------------===//
enum class JoinType {
	INVALID = 0, // invalid join type
	LEFT = 1,    // left
	RIGHT = 2,   // right
	INNER = 3,   // inner
	OUTER = 4,   // outer
	SEMI = 5     // IN+Subquery is SEMI
};

//===--------------------------------------------------------------------===//
// ORDER BY Clause Types
//===--------------------------------------------------------------------===//
enum class OrderType { INVALID = 0, ASCENDING = 1, DESCENDING = 2 };

//===--------------------------------------------------------------------===//
// Logical Operator Types
//===--------------------------------------------------------------------===//
enum class LogicalOperatorType {
	INVALID = 0,
	LEAF = 1,
	GET = 2,
	EXTERNAL_FILE_GET = 3,
	QUERY_DERIVED_GET = 4,
	PROJECTION = 5,
	FILTER = 6,
	AGGREGATE_AND_GROUP_BY = 7,
	DISTINCT = 8,
	LIMIT = 9,
	ORDER_BY = 10,
	// -----------------------------
	// Joins
	// -----------------------------
	MARK_JOIN = 100,
	DEPENDENT_JOIN = 101,
	SINGLE_JOIN = 102,
	INNER_JOIN = 103,
	LEFT_JOIN = 104,
	RIGHT_JOIN = 105,
	OUTER_JOIN = 106,
	SEMI_JOIN = 107,
	// -----------------------------
	// Updates
	// -----------------------------
	INSERT = 200,
	INSERT_SELECT = 201,
	DELETE = 202,
	UPDATE = 203,
	EXPORT_EXTERNAL_FILE = 204
};

//===--------------------------------------------------------------------===//
// Physical Operator Types
//===--------------------------------------------------------------------===//
enum class PhysicalOperatorType {
	INVALID = 0,
	LEAF = 1,
	DUMMY_SCAN = 2,
	SEQ_SCAN = 3,
	INDEX_SCAN = 4,
	EXTERNAL_FILE_SCAN = 5,
	QUERY_DERIVED_SCAN = 6,
	ORDER_BY = 7,
	LIMIT = 8,
	DISTINCT = 9,
	AGGREGATE = 10,
	HASH_GROUP_BY = 11,
	SORT_GROUP_BY = 12,
	FILTER = 13,
	PROJECTION = 14,
	BASE_GROUP_BY = 15,
	// -----------------------------
	// Joins
	// -----------------------------
	INNER_NL_JOIN = 100,
	LEFT_NL_JOIN = 101,
	RIGHT_NL_JOIN = 102,
	OUTER_NL_JOIN = 103,
	INNER_HASH_JOIN = 104,
	LEFT_HASH_JOIN = 105,
	RIGHT_HASH_JOIN = 106,
	OUTER_HASH_JOIN = 107,
	// -----------------------------
	// Updates
	// -----------------------------
	INSERT = 200,
	INSERT_SELECT = 201,
	DELETE = 202,
	UPDATE = 203,
	EXPORT_EXTERNAL_FILE = 204
};

ExpressionType StringToExpressionType(const std::string &str);

std::string TypeIdToString(TypeId type);
TypeId StringToTypeId(const std::string &str);
size_t GetTypeIdSize(TypeId type);

std::string LogicalOperatorToString(LogicalOperatorType type);
std::string PhysicalOperatorToString(PhysicalOperatorType type);
}
