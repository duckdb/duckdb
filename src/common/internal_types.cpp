
#include "common/internal_types.hpp"
#include "common/exception.hpp"
#include "common/string_util.hpp"

using namespace std;

namespace duckdb {

ExpressionType StringToExpressionType(const string &str) {
	string upper_str = StringUtil::Upper(str);
	if (upper_str == "INVALID") {
		return ExpressionType::INVALID;
	} else if (upper_str == "OPERATOR_PLUS" || upper_str == "+") {
		return ExpressionType::OPERATOR_ADD;
	} else if (upper_str == "OPERATOR_MINUS" || upper_str == "-") {
		return ExpressionType::OPERATOR_SUBTRACT;
	} else if (upper_str == "OPERATOR_MULTIPLY" || upper_str == "*") {
		return ExpressionType::OPERATOR_MULTIPLY;
	} else if (upper_str == "OPERATOR_DIVIDE" || upper_str == "/") {
		return ExpressionType::OPERATOR_DIVIDE;
	} else if (upper_str == "OPERATOR_CONCAT" || upper_str == "||") {
		return ExpressionType::OPERATOR_CONCAT;
	} else if (upper_str == "OPERATOR_MOD" || upper_str == "%") {
		return ExpressionType::OPERATOR_MOD;
	} else if (upper_str == "OPERATOR_CAST") {
		return ExpressionType::OPERATOR_CAST;
	} else if (upper_str == "OPERATOR_NOT") {
		return ExpressionType::OPERATOR_NOT;
	} else if (upper_str == "OPERATOR_IS_NULL") {
		return ExpressionType::OPERATOR_IS_NULL;
	} else if (upper_str == "OPERATOR_EXISTS") {
		return ExpressionType::OPERATOR_EXISTS;
	} else if (upper_str == "OPERATOR_UNARY_MINUS") {
		return ExpressionType::OPERATOR_UNARY_MINUS;
	} else if (upper_str == "COMPARE_EQUAL" || upper_str == "=") {
		return ExpressionType::COMPARE_EQUAL;
	} else if (upper_str == "COMPARE_NOTEQUAL" || upper_str == "!=" ||
	           upper_str == "<>") {
		return ExpressionType::COMPARE_NOTEQUAL;
	} else if (upper_str == "COMPARE_LESSTHAN" || upper_str == "<") {
		return ExpressionType::COMPARE_LESSTHAN;
	} else if (upper_str == "COMPARE_GREATERTHAN" || upper_str == ">") {
		return ExpressionType::COMPARE_GREATERTHAN;
	} else if (upper_str == "COMPARE_LESSTHANOREQUALTO" || upper_str == "<=") {
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	} else if (upper_str == "COMPARE_GREATERTHANOREQUALTO" ||
	           upper_str == ">=") {
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	} else if (upper_str == "COMPARE_LIKE" || upper_str == "~~") {
		return ExpressionType::COMPARE_LIKE;
	} else if (upper_str == "COMPARE_NOTLIKE" || upper_str == "!~~") {
		return ExpressionType::COMPARE_NOTLIKE;
	} else if (upper_str == "COMPARE_IN") {
		return ExpressionType::COMPARE_IN;
	} else if (upper_str == "COMPARE_DISTINCT_FROM") {
		return ExpressionType::COMPARE_DISTINCT_FROM;
	} else if (upper_str == "CONJUNCTION_AND") {
		return ExpressionType::CONJUNCTION_AND;
	} else if (upper_str == "CONJUNCTION_OR") {
		return ExpressionType::CONJUNCTION_OR;
	} else if (upper_str == "VALUE_CONSTANT") {
		return ExpressionType::VALUE_CONSTANT;
	} else if (upper_str == "VALUE_PARAMETER") {
		return ExpressionType::VALUE_PARAMETER;
	} else if (upper_str == "VALUE_TUPLE") {
		return ExpressionType::VALUE_TUPLE;
	} else if (upper_str == "VALUE_TUPLE_ADDRESS") {
		return ExpressionType::VALUE_TUPLE_ADDRESS;
	} else if (upper_str == "VALUE_NULL") {
		return ExpressionType::VALUE_NULL;
	} else if (upper_str == "VALUE_VECTOR") {
		return ExpressionType::VALUE_VECTOR;
	} else if (upper_str == "VALUE_SCALAR") {
		return ExpressionType::VALUE_SCALAR;
	} else if (upper_str == "AGGREGATE_COUNT") {
		return ExpressionType::AGGREGATE_COUNT;
	} else if (upper_str == "AGGREGATE_COUNT_STAR") {
		return ExpressionType::AGGREGATE_COUNT_STAR;
	} else if (upper_str == "AGGREGATE_SUM") {
		return ExpressionType::AGGREGATE_SUM;
	} else if (upper_str == "AGGREGATE_MIN") {
		return ExpressionType::AGGREGATE_MIN;
	} else if (upper_str == "AGGREGATE_MAX") {
		return ExpressionType::AGGREGATE_MAX;
	} else if (upper_str == "AGGREGATE_AVG") {
		return ExpressionType::AGGREGATE_AVG;
	} else if (upper_str == "FUNCTION") {
		return ExpressionType::FUNCTION;
	} else if (upper_str == "HASH_RANGE") {
		return ExpressionType::HASH_RANGE;
	} else if (upper_str == "OPERATOR_CASE_EXPR") {
		return ExpressionType::OPERATOR_CASE_EXPR;
	} else if (upper_str == "OPERATOR_NULLIF") {
		return ExpressionType::OPERATOR_NULLIF;
	} else if (upper_str == "OPERATOR_COALESCE") {
		return ExpressionType::OPERATOR_COALESCE;
	} else if (upper_str == "ROW_SUBQUERY") {
		return ExpressionType::ROW_SUBQUERY;
	} else if (upper_str == "SELECT_SUBQUERY") {
		return ExpressionType::SELECT_SUBQUERY;
	} else if (upper_str == "STAR") {
		return ExpressionType::STAR;
	} else if (upper_str == "PLACEHOLDER") {
		return ExpressionType::PLACEHOLDER;
	} else if (upper_str == "COLUMN_REF") {
		return ExpressionType::COLUMN_REF;
	} else if (upper_str == "FUNCTION_REF") {
		return ExpressionType::FUNCTION_REF;
	} else if (upper_str == "CAST") {
		return ExpressionType::CAST;
	}
	return ExpressionType::INVALID;
}

//===--------------------------------------------------------------------===//
// Value <--> String Utilities
//===--------------------------------------------------------------------===//

string TypeIdToString(TypeId type) {
	switch (type) {
	case TypeId::INVALID:
		return "INVALID";
	case TypeId::PARAMETER_OFFSET:
		return "PARAMETER_OFFSET";
	case TypeId::BOOLEAN:
		return "BOOLEAN";
	case TypeId::TINYINT:
		return "TINYINT";
	case TypeId::SMALLINT:
		return "SMALLINT";
	case TypeId::INTEGER:
		return "INTEGER";
	case TypeId::BIGINT:
		return "BIGINT";
	case TypeId::DECIMAL:
		return "DECIMAL";
	case TypeId::POINTER:
		return "POINTER";
	case TypeId::TIMESTAMP:
		return "TIMESTAMP";
	case TypeId::DATE:
		return "DATE";
	case TypeId::VARCHAR:
		return "VARCHAR";
	case TypeId::VARBINARY:
		return "VARBINARY";
	case TypeId::ARRAY:
		return "ARRAY";
	case TypeId::UDT:
		return "UDT";
	}
	return "INVALID";
}

TypeId StringToTypeId(const string &str) {
	string upper_str = StringUtil::Upper(str);
	if (upper_str == "INVALID") {
		return TypeId::INVALID;
	} else if (upper_str == "PARAMETER_OFFSET") {
		return TypeId::PARAMETER_OFFSET;
	} else if (upper_str == "BOOLEAN") {
		return TypeId::BOOLEAN;
	} else if (upper_str == "TINYINT") {
		return TypeId::TINYINT;
	} else if (upper_str == "SMALLINT") {
		return TypeId::SMALLINT;
	} else if (upper_str == "INTEGER") {
		return TypeId::INTEGER;
	} else if (upper_str == "BIGINT") {
		return TypeId::BIGINT;
	} else if (upper_str == "DECIMAL") {
		return TypeId::DECIMAL;
	} else if (upper_str == "POINTER") {
		return TypeId::POINTER;
	} else if (upper_str == "TIMESTAMP") {
		return TypeId::TIMESTAMP;
	} else if (upper_str == "DATE") {
		return TypeId::DATE;
	} else if (upper_str == "VARCHAR") {
		return TypeId::VARCHAR;
	} else if (upper_str == "VARBINARY") {
		return TypeId::VARBINARY;
	} else if (upper_str == "ARRAY") {
		return TypeId::ARRAY;
	} else if (upper_str == "UDT") {
		return TypeId::UDT;
	}
	return TypeId::INVALID;
}

size_t GetTypeIdSize(TypeId type) {
	switch (type) {
	case TypeId::PARAMETER_OFFSET:
		return sizeof(uint64_t);
	case TypeId::BOOLEAN:
		return sizeof(bool);
	case TypeId::TINYINT:
		return sizeof(int8_t);
	case TypeId::SMALLINT:
		return sizeof(int16_t);
	case TypeId::INTEGER:
		return sizeof(int32_t);
	case TypeId::BIGINT:
		return sizeof(int64_t);
	case TypeId::DECIMAL:
		return sizeof(double);
	case TypeId::POINTER:
		return sizeof(uint64_t);
	case TypeId::TIMESTAMP:
		return sizeof(int64_t);
	case TypeId::DATE:
		return sizeof(int32_t);
	case TypeId::VARCHAR:
		return sizeof(void *);
	case TypeId::VARBINARY:
		return sizeof(void *);
	case TypeId::ARRAY:
		return sizeof(void *);
	case TypeId::UDT:
		return sizeof(void *);
	default:
		throw Exception("Invalid type ID size!");
		return (size_t)-1;
	}
}

string LogicalOperatorToString(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::LEAF:
		return "LEAF";
	case LogicalOperatorType::GET:
		return "GET";
	case LogicalOperatorType::EXTERNAL_FILE_GET:
		return "EXTERNAL_FILE_GET";
	case LogicalOperatorType::QUERY_DERIVED_GET:
		return "QUERY_DERIVED_GET";
	case LogicalOperatorType::PROJECTION:
		return "PROJECTION";
	case LogicalOperatorType::FILTER:
		return "FILTER";
	case LogicalOperatorType::AGGREGATE_AND_GROUP_BY:
		return "AGGREGATE_AND_GROUP_BY";
	case LogicalOperatorType::DISTINCT:
		return "DISTINCT";
	case LogicalOperatorType::LIMIT:
		return "LIMIT";
	case LogicalOperatorType::ORDER_BY:
		return "ORDER_BY";
	case LogicalOperatorType::MARK_JOIN:
		return "MARK_JOIN";
	case LogicalOperatorType::DEPENDENT_JOIN:
		return "DEPENDENT_JOIN";
	case LogicalOperatorType::SINGLE_JOIN:
		return "SINGLE_JOIN";
	case LogicalOperatorType::INNER_JOIN:
		return "INNER_JOIN";
	case LogicalOperatorType::LEFT_JOIN:
		return "LEFT_JOIN";
	case LogicalOperatorType::RIGHT_JOIN:
		return "RIGHT_JOIN";
	case LogicalOperatorType::OUTER_JOIN:
		return "OUTER_JOIN";
	case LogicalOperatorType::SEMI_JOIN:
		return "SEMI_JOIN";
	case LogicalOperatorType::INSERT:
		return "INSERT";
	case LogicalOperatorType::INSERT_SELECT:
		return "INSERT_SELECT";
	case LogicalOperatorType::DELETE:
		return "DELETE";
	case LogicalOperatorType::UPDATE:
		return "UPDATE";
	case LogicalOperatorType::EXPORT_EXTERNAL_FILE:
		return "EXPORT_EXTERNAL_FILE";
	default:
		return "INVALID";
	}
}

string PhysicalOperatorToString(PhysicalOperatorType type) {
	switch (type) {
	case PhysicalOperatorType::LEAF:
		return "LEAF";
	case PhysicalOperatorType::DUMMY_SCAN:
		return "DUMMY_SCAN";
	case PhysicalOperatorType::SEQ_SCAN:
		return "SEQ_SCAN";
	case PhysicalOperatorType::INDEX_SCAN:
		return "INDEX_SCAN";
	case PhysicalOperatorType::EXTERNAL_FILE_SCAN:
		return "EXTERNAL_FILE_SCAN";
	case PhysicalOperatorType::QUERY_DERIVED_SCAN:
		return "QUERY_DERIVED_SCAN";
	case PhysicalOperatorType::ORDER_BY:
		return "ORDER_BY";
	case PhysicalOperatorType::LIMIT:
		return "LIMIT";
	case PhysicalOperatorType::DISTINCT:
		return "DISTINCT";
	case PhysicalOperatorType::AGGREGATE:
		return "AGGREGATE";
	case PhysicalOperatorType::HASH_GROUP_BY:
		return "HASH_GROUP_BY";
	case PhysicalOperatorType::SORT_GROUP_BY:
		return "SORT_GROUP_BY";
	case PhysicalOperatorType::FILTER:
		return "FILTER";
	case PhysicalOperatorType::INNER_NL_JOIN:
		return "INNER_NL_JOIN";
	case PhysicalOperatorType::LEFT_NL_JOIN:
		return "LEFT_NL_JOIN";
	case PhysicalOperatorType::RIGHT_NL_JOIN:
		return "RIGHT_NL_JOIN";
	case PhysicalOperatorType::OUTER_NL_JOIN:
		return "OUTER_NL_JOIN";
	case PhysicalOperatorType::INNER_HASH_JOIN:
		return "INNER_HASH_JOIN";
	case PhysicalOperatorType::LEFT_HASH_JOIN:
		return "LEFT_HASH_JOIN";
	case PhysicalOperatorType::RIGHT_HASH_JOIN:
		return "RIGHT_HASH_JOIN";
	case PhysicalOperatorType::OUTER_HASH_JOIN:
		return "OUTER_HASH_JOIN";
	case PhysicalOperatorType::INSERT:
		return "INSERT";
	case PhysicalOperatorType::INSERT_SELECT:
		return "INSERT_SELECT";
	case PhysicalOperatorType::DELETE:
		return "DELETE";
	case PhysicalOperatorType::UPDATE:
		return "UPDATE";
	case PhysicalOperatorType::EXPORT_EXTERNAL_FILE:
		return "EXPORT_EXTERNAL_FILE";
	default:
		return "INVALID";
	}
}
}; // namespace duckdb
