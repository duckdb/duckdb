#include "common/enums/expression_type.hpp"

#include "common/exception.hpp"

using namespace std;

namespace duckdb {

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
	case ExpressionType::COMPARE_SIMILAR:
		return "SIMILAR";
	case ExpressionType::COMPARE_NOTSIMILAR:
		return "NOTSIMILAR";
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
	default:
		return "INVALID";
	}
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
		return "||";
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

ExpressionType NegateComparisionExpression(ExpressionType type) {
	ExpressionType negated_type = ExpressionType::INVALID;
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		negated_type = ExpressionType::COMPARE_NOTEQUAL;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		negated_type = ExpressionType::COMPARE_EQUAL;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		negated_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		negated_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		negated_type = ExpressionType::COMPARE_GREATERTHAN;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		negated_type = ExpressionType::COMPARE_LESSTHAN;
		break;

	default:
		throw Exception("Unsupported comparison type in negation");
	}
	return negated_type;
}

ExpressionType FlipComparisionExpression(ExpressionType type) {
	ExpressionType flipped_type = ExpressionType::INVALID;
	switch (type) {
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_EQUAL:
		flipped_type = type;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		flipped_type = ExpressionType::COMPARE_GREATERTHAN;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		flipped_type = ExpressionType::COMPARE_LESSTHAN;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		flipped_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		flipped_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
		break;

	default:
		throw Exception("Unsupported comparison type in flip");
	}
	return flipped_type;
}

} // namespace duckdb
