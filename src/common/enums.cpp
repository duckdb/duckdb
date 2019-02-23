#include "common/enums.hpp"

#include "common/exception.hpp"
#include "common/string_util.hpp"

using namespace std;

#include <unordered_map>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Value <--> String Utilities
//===--------------------------------------------------------------------===//

string TypeIdToString(TypeId type) {
	switch (type) {
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
	case TypeId::INVALID:
		break;
	}
	return "INVALID";
}

string LogicalOperatorToString(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::GET:
		return "GET";
	case LogicalOperatorType::CHUNK_GET:
		return "CHUNK_GET";
	case LogicalOperatorType::DELIM_JOIN:
		return "DELIM_JOIN";
	case LogicalOperatorType::PROJECTION:
		return "PROJECTION";
	case LogicalOperatorType::FILTER:
		return "FILTER";
	case LogicalOperatorType::AGGREGATE_AND_GROUP_BY:
		return "AGGREGATE_AND_GROUP_BY";
	case LogicalOperatorType::WINDOW:
		return "WINDOW";
	case LogicalOperatorType::LIMIT:
		return "LIMIT";
	case LogicalOperatorType::ORDER_BY:
		return "ORDER_BY";
	case LogicalOperatorType::COPY:
		return "COPY";
	case LogicalOperatorType::SUBQUERY:
		return "SUBQUERY";
	case LogicalOperatorType::JOIN:
		return "JOIN";
	case LogicalOperatorType::CROSS_PRODUCT:
		return "CROSS_PRODUCT";
	case LogicalOperatorType::UNION:
		return "UNION";
	case LogicalOperatorType::EXCEPT:
		return "EXCEPT";
	case LogicalOperatorType::INTERSECT:
		return "INTERSECT";
	case LogicalOperatorType::INSERT:
		return "INSERT";
	case LogicalOperatorType::DISTINCT:
		return "DISTINCT";
	case LogicalOperatorType::DELETE:
		return "DELETE";
	case LogicalOperatorType::UPDATE:
		return "UPDATE";
	case LogicalOperatorType::PRUNE_COLUMNS:
		return "PRUNE";
	case LogicalOperatorType::TABLE_FUNCTION:
		return "TABLE_FUNCTION";
	case LogicalOperatorType::CREATE_INDEX:
		return "CREATE_INDEX";
	case LogicalOperatorType::CREATE_TABLE:
		return "CREATE_TABLE";
	case LogicalOperatorType::EXPLAIN:
		return "EXPLAIN";
	case LogicalOperatorType::INVALID:
		break;
	}
	return "INVALID";
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
	case PhysicalOperatorType::CHUNK_SCAN:
		return "CHUNK_SCAN";
	case PhysicalOperatorType::EXTERNAL_FILE_SCAN:
		return "EXTERNAL_FILE_SCAN";
	case PhysicalOperatorType::QUERY_DERIVED_SCAN:
		return "QUERY_DERIVED_SCAN";
	case PhysicalOperatorType::ORDER_BY:
		return "ORDER_BY";
	case PhysicalOperatorType::LIMIT:
		return "LIMIT";
	case PhysicalOperatorType::AGGREGATE:
		return "AGGREGATE";
	case PhysicalOperatorType::WINDOW:
		return "WINDOW";
	case PhysicalOperatorType::DISTINCT:
		return "DISTINCT";
	case PhysicalOperatorType::HASH_GROUP_BY:
		return "HASH_GROUP_BY";
	case PhysicalOperatorType::SORT_GROUP_BY:
		return "SORT_GROUP_BY";
	case PhysicalOperatorType::FILTER:
		return "FILTER";
	case PhysicalOperatorType::PROJECTION:
		return "PROJECTION";
	case PhysicalOperatorType::BASE_GROUP_BY:
		return "BASE_GROUP_BY";
	case PhysicalOperatorType::COPY:
		return "COPY";
	case PhysicalOperatorType::DELIM_JOIN:
		return "DELIM_JOIN";
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
		return "NESTED_LOOP_JOIN";
	case PhysicalOperatorType::HASH_JOIN:
		return "HASH_JOIN";
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		return "PIECEWISE_MERGE_JOIN";
	case PhysicalOperatorType::CROSS_PRODUCT:
		return "CROSS_PRODUCT";
	case PhysicalOperatorType::UNION:
		return "UNION";
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
	case PhysicalOperatorType::PRUNE_COLUMNS:
		return "PRUNE";
	case PhysicalOperatorType::EMPTY_RESULT:
		return "EMPTY_RESULT";
	case PhysicalOperatorType::TABLE_FUNCTION:
		return "TABLE_FUNCTION";
	case PhysicalOperatorType::CREATE:
		return "CREATE";
	case PhysicalOperatorType::CREATE_INDEX:
		return "CREATE_INDEX";
	case PhysicalOperatorType::EXPLAIN:
		return "EXPLAIN";
	case PhysicalOperatorType::INVALID:
		break;
	}
	return "INVALID";
}

string ExpressionTypeToString(ExpressionType type) {
	switch (type) {
	case ExpressionType::OPERATOR_ADD:
		return "ADD";
	case ExpressionType::OPERATOR_SUBTRACT:
		return "SUBTRACT";
	case ExpressionType::OPERATOR_MULTIPLY:
		return "MULTIPLY";
	case ExpressionType::OPERATOR_DIVIDE:
		return "DIVIDE";
	case ExpressionType::OPERATOR_CONCAT:
		return "CONCAT";
	case ExpressionType::OPERATOR_MOD:
		return "MOD";
	case ExpressionType::OPERATOR_CAST:
		return "CAST";
	case ExpressionType::OPERATOR_NOT:
		return "NOT";
	case ExpressionType::OPERATOR_IS_NULL:
		return "IS_NULL";
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return "IS_NOT_NULL";
	case ExpressionType::COMPARE_EQUAL:
		return "EQUAL";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "NOTEQUAL";
	case ExpressionType::COMPARE_LESSTHAN:
		return "LESSTHAN";
	case ExpressionType::COMPARE_GREATERTHAN:
		return "GREATERTHAN";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "LESSTHANOREQUALTO";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return "GREATERTHANOREQUALTO";
	case ExpressionType::COMPARE_LIKE:
		return "LIKE";
	case ExpressionType::COMPARE_NOTLIKE:
		return "NOTLIKE";
	case ExpressionType::COMPARE_IN:
		return "IN";
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "DISTINCT_FROM";
	case ExpressionType::CONJUNCTION_AND:
		return "AND";
	case ExpressionType::CONJUNCTION_OR:
		return "OR";
	case ExpressionType::VALUE_CONSTANT:
		return "CONSTANT";
	case ExpressionType::VALUE_PARAMETER:
		return "PARAMETER";
	case ExpressionType::VALUE_TUPLE:
		return "TUPLE";
	case ExpressionType::VALUE_TUPLE_ADDRESS:
		return "TUPLE_ADDRESS";
	case ExpressionType::VALUE_NULL:
		return "NULL";
	case ExpressionType::VALUE_VECTOR:
		return "VECTOR";
	case ExpressionType::VALUE_SCALAR:
		return "SCALAR";
	case ExpressionType::AGGREGATE_COUNT:
		return "COUNT";
	case ExpressionType::AGGREGATE_COUNT_STAR:
		return "COUNT_STAR";
	case ExpressionType::AGGREGATE_COUNT_DISTINCT:
		return "COUNT_DISTINCT";
	case ExpressionType::AGGREGATE_SUM:
		return "SUM";
	case ExpressionType::AGGREGATE_SUM_DISTINCT:
		return "SUM_DISTINCT";
	case ExpressionType::AGGREGATE_MIN:
		return "MIN";
	case ExpressionType::AGGREGATE_MAX:
		return "MAX";
	case ExpressionType::AGGREGATE_AVG:
		return "AVG";
	case ExpressionType::AGGREGATE_FIRST:
		return "FIRST";
	case ExpressionType::AGGREGATE_STDDEV_SAMP:
		return "AGGREGATE_STDDEV_SAMP";
	case ExpressionType::WINDOW_SUM:
		return "SUM";
	case ExpressionType::WINDOW_COUNT_STAR:
		return "COUNT_STAR";
	case ExpressionType::WINDOW_MIN:
		return "MIN";
	case ExpressionType::WINDOW_MAX:
		return "MAX";
	case ExpressionType::WINDOW_AVG:
		return "AVG";
	case ExpressionType::WINDOW_RANK:
		return "RANK";
	case ExpressionType::WINDOW_RANK_DENSE:
		return "RANK_DENSE";
	case ExpressionType::WINDOW_PERCENT_RANK:
		return "PERCENT_RANK";
	case ExpressionType::WINDOW_ROW_NUMBER:
		return "ROW_NUMBER";
	case ExpressionType::WINDOW_FIRST_VALUE:
		return "FIRST_VALUE";
	case ExpressionType::WINDOW_LAST_VALUE:
		return "LAST_VALUE";
	case ExpressionType::WINDOW_CUME_DIST:
		return "CUME_DIST";
	case ExpressionType::WINDOW_LEAD:
		return "LEAD";
	case ExpressionType::WINDOW_LAG:
		return "LAG";
	case ExpressionType::WINDOW_NTILE:
		return "NTILE";
	case ExpressionType::FUNCTION:
		return "FUNCTION";
	case ExpressionType::OPERATOR_CASE_EXPR:
		return "CASE";
	case ExpressionType::OPERATOR_NULLIF:
		return "NULLIF";
	case ExpressionType::OPERATOR_COALESCE:
		return "COALESCE";
	case ExpressionType::SUBQUERY:
		return "SUBQUERY";
	case ExpressionType::STAR:
		return "STAR";
	case ExpressionType::PLACEHOLDER:
		return "PLACEHOLDER";
	case ExpressionType::COLUMN_REF:
		return "COLUMN_REF";
	case ExpressionType::FUNCTION_REF:
		return "FUNCTION_REF";
	case ExpressionType::TABLE_REF:
		return "TABLE_REF";
	case ExpressionType::CAST:
		return "CAST";
	case ExpressionType::COMPARE_NOT_IN:
		return "COMPARE_NOT_IN";
	case ExpressionType::COMPARE_BETWEEN:
		return "COMPARE_BETWEEN";
	case ExpressionType::COMPARE_NOT_BETWEEN:
		return "COMPARE_NOT_BETWEEN";
	case ExpressionType::VALUE_DEFAULT:
		return "VALUE_DEFAULT";
	case ExpressionType::COMMON_SUBEXPRESSION:
		return "COMMON_SUBEXPRESSION";
	case ExpressionType::BOUND_REF:
		return "BOUND_REF";
	case ExpressionType::BOUND_COLUMN_REF:
		return "BOUND_COLUMN_REF";
	case ExpressionType::BOUND_FUNCTION:
		return "BOUND_FUNCTION";
	case ExpressionType::INVALID:
		break;
	}
	return "INVALID";
}

string ExpressionTypeToOperator(ExpressionType type) {
	switch (type) {
	case ExpressionType::OPERATOR_ADD:
		return "+";
	case ExpressionType::OPERATOR_SUBTRACT:
		return "-";
	case ExpressionType::OPERATOR_MULTIPLY:
		return "*";
	case ExpressionType::OPERATOR_DIVIDE:
		return "/";
	case ExpressionType::OPERATOR_CONCAT:
		return "+";
	case ExpressionType::OPERATOR_MOD:
		return "%";
	case ExpressionType::OPERATOR_NOT:
		return "!";
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	case ExpressionType::CONJUNCTION_AND:
		return "AND";
	case ExpressionType::CONJUNCTION_OR:
		return "OR";
	case ExpressionType::STAR:
		return "*";
	default:
		return "";
	}
}

string JoinTypeToString(JoinType type) {
	switch (type) {
	case JoinType::LEFT:
		return "LEFT";
	case JoinType::RIGHT:
		return "RIGHT";
	case JoinType::INNER:
		return "INNER";
	case JoinType::OUTER:
		return "OUTER";
	case JoinType::SEMI:
		return "SEMI";
	case JoinType::ANTI:
		return "ANTI";
	case JoinType::SINGLE:
		return "SINGLE";
	case JoinType::MARK:
		return "MARK";
	case JoinType::INVALID:
		break;
	}
	return "INVALID";
}

ExternalFileFormat StringToExternalFileFormat(const string &str) {
	auto upper = StringUtil::Upper(str);
	if (upper == "CSV") {
		return ExternalFileFormat::CSV;
	}
	throw ConversionException("No ExternalFileFormat for input '%s'", upper.c_str());
}

IndexType StringToIndexType(const string &str) {
	string upper_str = StringUtil::Upper(str);
	if (upper_str == "INVALID") {
		return IndexType::INVALID;
	} else if (upper_str == "BTREE") {
		return IndexType::BTREE;
	} else {
		throw ConversionException(StringUtil::Format("No IndexType conversion from string '%s'", upper_str.c_str()));
	}
	return IndexType::INVALID;
}

} // namespace duckdb
