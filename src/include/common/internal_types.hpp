//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/internal_types.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bitset>
#include <cassert>
#include <cstring>
#include <limits>
#include <math.h>
#include <memory>
#include <string>

namespace duckdb {

#define DEFAULT_SCHEMA "main"

#define STANDARD_VECTOR_SIZE 1024
#define STORAGE_CHUNK_SIZE 10240

//! Type used to represent dates
typedef int32_t date_t;
//! Type used to represent timestamps
typedef int64_t timestamp_t;
//! Type used for the selection vector
typedef uint16_t sel_t;
//! Type used for transaction timestamps
//! FIXME: this should be a 128-bit integer
//! With 64-bit, the database only supports up to 2^32 transactions
typedef uint64_t transaction_t;
//! Type used for nullmasks
typedef std::bitset<STANDARD_VECTOR_SIZE> nullmask_t;

//! Type used for column identifiers
typedef size_t column_t;
//! Special value used to signify the ROW ID of
extern column_t COLUMN_IDENTIFIER_ROW_ID;

//! Zero selection vector: completely filled with the value 0 [READ ONLY]
extern sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE];
//! Zero NULL mask: filled with the value 0 [READ ONLY]
extern nullmask_t ZERO_MASK;

struct BinaryData {
	std::unique_ptr<uint8_t[]> data;
	size_t size;
};

//===--------------------------------------------------------------------===//
// SQL Value Types
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
	AGGREGATE_SUM = 102,
	AGGREGATE_MIN = 103,
	AGGREGATE_MAX = 104,
	AGGREGATE_AVG = 105,
	AGGREGATE_FIRST = 106,

	// -----------------------------
	// Functions
	// -----------------------------
	FUNCTION = 125,

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
	GROUP_REF = 205,

	// -----------------------------
	// Miscellaneous
	// -----------------------------
	CAST = 225
};

//===--------------------------------------------------------------------===//
// Expression Class
//===--------------------------------------------------------------------===//
enum class ExpressionClass : uint8_t {
	INVALID = 0,
	AGGREGATE = 1,
	ALIAS_REF = 2,
	CASE = 3,
	CAST = 4,
	COLUMN_REF = 5,
	COMPARISON = 6,
	CONJUNCTION = 7,
	CONSTANT = 8,
	DEFAULT = 9,
	FUNCTION = 10,
	GROUP_REF = 11,
	OPERATOR = 12,
	STAR = 13,
	SUBQUERY = 14
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
	DELETED_ENTRY = 10
};

//===--------------------------------------------------------------------===//
// Subquery Types
//===--------------------------------------------------------------------===//
enum class SubqueryType : uint8_t {
	INVALID = 0,
	DEFAULT = 1,
	EXISTS = 2,
	IN = 3
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
	PREPARE,      // prepare statement type
	EXECUTE,      // execute statement type
	RENAME,       // rename statement type
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

	// -----------------------------
	// Drop Types
	// -----------------------------
	DROP_TABLE,  // drop table statement type
	DROP_SCHEMA, // drop table statement type

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
// ORDER BY Clause Types
//===--------------------------------------------------------------------===//
enum class OrderType : uint8_t { INVALID = 0, ASCENDING = 1, DESCENDING = 2 };

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

	// -----------------------------
	// Updates
	// -----------------------------
	INSERT,
	INSERT_SELECT,
	DELETE,
	UPDATE,
	EXPORT_EXTERNAL_FILE,
	CREATE,

	// -----------------------------
	// Explain
	// -----------------------------
	EXPLAIN
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
	CREATE
};

//===--------------------------------------------------------------------===//
// Match Order
//===--------------------------------------------------------------------===//
enum class MatchOrder : uint8_t { ARBITRARY, DEPTH_FIRST };
//===--------------------------------------------------------------------===//
// Child Match Policy
//===--------------------------------------------------------------------===//
enum class ChildPolicy : uint8_t {
	ALWAYS_MATCH,
	ANY,
	LEAF,
	SOME,
	UNORDERED,
	ORDERED
};

//===--------------------------------------------------------------------===//
// External File Format Types
//===--------------------------------------------------------------------===//
enum class ExternalFileFormat : uint8_t { INVALID, CSV };

//===--------------------------------------------------------------------===//
// Transaction Operation Types
//===--------------------------------------------------------------------===//
enum class TransactionType : uint8_t {
	INVALID,
	BEGIN_TRANSACTION,
	COMMIT,
	ROLLBACK
};

ExpressionType StringToExpressionType(const std::string &str);

std::string TypeIdToString(TypeId type);
TypeId StringToTypeId(const std::string &str);
size_t GetTypeIdSize(TypeId type);
bool TypeIsConstantSize(TypeId type);
bool TypeIsIntegral(TypeId type);
bool TypeIsNumeric(TypeId type);

//! Returns the minimum value that can be stored in a given type
int64_t MinimumValue(TypeId type);
//! Returns the maximum value that can be stored in a given type
int64_t MaximumValue(TypeId type);
//! Returns the minimal type that guarantees an integer value from not
//! overflowing
TypeId MinimalType(int64_t value);

std::string LogicalOperatorToString(LogicalOperatorType type);
std::string PhysicalOperatorToString(PhysicalOperatorType type);
std::string ExpressionTypeToString(ExpressionType type);

ExternalFileFormat StringToExternalFileFormat(const std::string &str);
} // namespace duckdb
