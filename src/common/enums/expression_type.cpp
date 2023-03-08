#include "duckdb/common/enums/expression_type.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/enum_serializer.hpp"

namespace duckdb {
// LCOV_EXCL_START
template <>
const char *EnumSerializer::EnumToString(ExpressionType value) {
	switch (value) {
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
	case ExpressionType::COMPARE_IN:
		return "IN";
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "DISTINCT_FROM";
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return "NOT_DISTINCT_FROM";
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
	case ExpressionType::AGGREGATE:
		return "AGGREGATE";
	case ExpressionType::WINDOW_AGGREGATE:
		return "WINDOW_AGGREGATE";
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
	case ExpressionType::WINDOW_NTH_VALUE:
		return "NTH_VALUE";
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
	case ExpressionType::CASE_EXPR:
		return "CASE";
	case ExpressionType::OPERATOR_NULLIF:
		return "NULLIF";
	case ExpressionType::OPERATOR_COALESCE:
		return "COALESCE";
	case ExpressionType::ARRAY_EXTRACT:
		return "ARRAY_EXTRACT";
	case ExpressionType::ARRAY_SLICE:
		return "ARRAY_SLICE";
	case ExpressionType::STRUCT_EXTRACT:
		return "STRUCT_EXTRACT";
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
	case ExpressionType::BOUND_REF:
		return "BOUND_REF";
	case ExpressionType::BOUND_COLUMN_REF:
		return "BOUND_COLUMN_REF";
	case ExpressionType::BOUND_FUNCTION:
		return "BOUND_FUNCTION";
	case ExpressionType::BOUND_AGGREGATE:
		return "BOUND_AGGREGATE";
	case ExpressionType::GROUPING_FUNCTION:
		return "GROUPING";
	case ExpressionType::ARRAY_CONSTRUCTOR:
		return "ARRAY_CONSTRUCTOR";
	case ExpressionType::TABLE_STAR:
		return "TABLE_STAR";
	case ExpressionType::BOUND_UNNEST:
		return "BOUND_UNNEST";
	case ExpressionType::COLLATE:
		return "COLLATE";
	case ExpressionType::POSITIONAL_REFERENCE:
		return "POSITIONAL_REFERENCE";
	case ExpressionType::BOUND_LAMBDA_REF:
		return "BOUND_LAMBDA_REF";
	case ExpressionType::LAMBDA:
		return "LAMBDA";
	case ExpressionType::ARROW:
		return "ARROW";
	case ExpressionType::INVALID:
		break;
	}
	return "INVALID";
}
// LCOV_EXCL_STOP

string ExpressionTypeToString(ExpressionType type) {
	return EnumSerializer::EnumToString(type);
}

template <>
ExpressionType EnumSerializer::StringToEnum(const char *value) {
	if (strcmp(value, "CAST") == 0) {
		return ExpressionType::OPERATOR_CAST;
	} else if (strcmp(value, "NOT") == 0) {
		return ExpressionType::OPERATOR_NOT;
	} else if (strcmp(value, "IS_NULL") == 0) {
		return ExpressionType::OPERATOR_IS_NULL;
	} else if (strcmp(value, "IS_NOT_NULL") == 0) {
		return ExpressionType::OPERATOR_IS_NOT_NULL;
	} else if (strcmp(value, "EQUAL") == 0) {
		return ExpressionType::COMPARE_EQUAL;
	} else if (strcmp(value, "NOTEQUAL") == 0) {
		return ExpressionType::COMPARE_NOTEQUAL;
	} else if (strcmp(value, "LESSTHAN") == 0) {
		return ExpressionType::COMPARE_LESSTHAN;
	} else if (strcmp(value, "GREATERTHAN") == 0) {
		return ExpressionType::COMPARE_GREATERTHAN;
	} else if (strcmp(value, "LESSTHANOREQUALTO") == 0) {
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	} else if (strcmp(value, "GREATERTHANOREQUALTO") == 0) {
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	} else if (strcmp(value, "IN") == 0) {
		return ExpressionType::COMPARE_IN;
	} else if (strcmp(value, "DISTINCT_FROM") == 0) {
		return ExpressionType::COMPARE_DISTINCT_FROM;
	} else if (strcmp(value, "NOT_DISTINCT_FROM") == 0) {
		return ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	} else if (strcmp(value, "AND") == 0) {
		return ExpressionType::CONJUNCTION_AND;
	} else if (strcmp(value, "OR") == 0) {
		return ExpressionType::CONJUNCTION_OR;
	} else if (strcmp(value, "CONSTANT") == 0) {
		return ExpressionType::VALUE_CONSTANT;
	} else if (strcmp(value, "PARAMETER") == 0) {
		return ExpressionType::VALUE_PARAMETER;
	} else if (strcmp(value, "TUPLE") == 0) {
		return ExpressionType::VALUE_TUPLE;
	} else if (strcmp(value, "TUPLE_ADDRESS") == 0) {
		return ExpressionType::VALUE_TUPLE_ADDRESS;
	} else if (strcmp(value, "NULL") == 0) {
		return ExpressionType::VALUE_NULL;
	} else if (strcmp(value, "VECTOR") == 0) {
		return ExpressionType::VALUE_VECTOR;
	} else if (strcmp(value, "SCALAR") == 0) {
		return ExpressionType::VALUE_SCALAR;
	} else if (strcmp(value, "AGGREGATE") == 0) {
		return ExpressionType::AGGREGATE;
	} else if (strcmp(value, "WINDOW_AGGREGATE") == 0) {
		return ExpressionType::WINDOW_AGGREGATE;
	} else if (strcmp(value, "RANK") == 0) {
		return ExpressionType::WINDOW_RANK;
	} else if (strcmp(value, "RANK_DENSE") == 0) {
		return ExpressionType::WINDOW_RANK_DENSE;
	} else if (strcmp(value, "PERCENT_RANK") == 0) {
		return ExpressionType::WINDOW_PERCENT_RANK;
	} else if (strcmp(value, "ROW_NUMBER") == 0) {
		return ExpressionType::WINDOW_ROW_NUMBER;
	} else if (strcmp(value, "FIRST_VALUE") == 0) {
		return ExpressionType::WINDOW_FIRST_VALUE;
	} else if (strcmp(value, "LAST_VALUE") == 0) {
		return ExpressionType::WINDOW_LAST_VALUE;
	} else if (strcmp(value, "NTH_VALUE") == 0) {
		return ExpressionType::WINDOW_NTH_VALUE;
	} else if (strcmp(value, "CUME_DIST") == 0) {
		return ExpressionType::WINDOW_CUME_DIST;
	} else if (strcmp(value, "LEAD") == 0) {
		return ExpressionType::WINDOW_LEAD;
	} else if (strcmp(value, "LAG") == 0) {
		return ExpressionType::WINDOW_LAG;
	} else if (strcmp(value, "NTILE") == 0) {
		return ExpressionType::WINDOW_NTILE;
	} else if (strcmp(value, "FUNCTION") == 0) {
		return ExpressionType::FUNCTION;
	} else if (strcmp(value, "CASE") == 0) {
		return ExpressionType::CASE_EXPR;
	} else if (strcmp(value, "NULLIF") == 0) {
		return ExpressionType::OPERATOR_NULLIF;
	} else if (strcmp(value, "COALESCE") == 0) {
		return ExpressionType::OPERATOR_COALESCE;
	} else if (strcmp(value, "ARRAY_EXTRACT") == 0) {
		return ExpressionType::ARRAY_EXTRACT;
	} else if (strcmp(value, "ARRAY_SLICE") == 0) {
		return ExpressionType::ARRAY_SLICE;
	} else if (strcmp(value, "STRUCT_EXTRACT") == 0) {
		return ExpressionType::STRUCT_EXTRACT;
	} else if (strcmp(value, "SUBQUERY") == 0) {
		return ExpressionType::SUBQUERY;
	} else if (strcmp(value, "STAR") == 0) {
		return ExpressionType::STAR;
	} else if (strcmp(value, "PLACEHOLDER") == 0) {
		return ExpressionType::PLACEHOLDER;
	} else if (strcmp(value, "COLUMN_REF") == 0) {
		return ExpressionType::COLUMN_REF;
	} else if (strcmp(value, "FUNCTION_REF") == 0) {
		return ExpressionType::FUNCTION_REF;
	} else if (strcmp(value, "TABLE_REF") == 0) {
		return ExpressionType::TABLE_REF;
	} else if (strcmp(value, "CAST") == 0) {
		return ExpressionType::CAST;
	} else if (strcmp(value, "COMPARE_NOT_IN") == 0) {
		return ExpressionType::COMPARE_NOT_IN;
	} else if (strcmp(value, "COMPARE_BETWEEN") == 0) {
		return ExpressionType::COMPARE_BETWEEN;
	} else if (strcmp(value, "COMPARE_NOT_BETWEEN") == 0) {
		return ExpressionType::COMPARE_NOT_BETWEEN;
	} else if (strcmp(value, "VALUE_DEFAULT") == 0) {
		return ExpressionType::VALUE_DEFAULT;
	} else if (strcmp(value, "BOUND_REF") == 0) {
		return ExpressionType::BOUND_REF;
	} else if (strcmp(value, "BOUND_COLUMN_REF") == 0) {
		return ExpressionType::BOUND_COLUMN_REF;
	} else if (strcmp(value, "BOUND_FUNCTION") == 0) {
		return ExpressionType::BOUND_FUNCTION;
	} else if (strcmp(value, "BOUND_AGGREGATE") == 0) {
		return ExpressionType::BOUND_AGGREGATE;
	} else if (strcmp(value, "GROUPING") == 0) {
		return ExpressionType::GROUPING_FUNCTION;
	} else if (strcmp(value, "ARRAY_CONSTRUCTOR") == 0) {
		return ExpressionType::ARRAY_CONSTRUCTOR;
	} else if (strcmp(value, "TABLE_STAR") == 0) {
		return ExpressionType::TABLE_STAR;
	} else if (strcmp(value, "BOUND_UNNEST") == 0) {
		return ExpressionType::BOUND_UNNEST;
	} else if (strcmp(value, "COLLATE") == 0) {
		return ExpressionType::COLLATE;
	} else if (strcmp(value, "POSITIONAL_REFERENCE") == 0) {
		return ExpressionType::POSITIONAL_REFERENCE;
	} else if (strcmp(value, "BOUND_LAMBDA_REF") == 0) {
		return ExpressionType::BOUND_LAMBDA_REF;
	} else if (strcmp(value, "LAMBDA") == 0) {
		return ExpressionType::LAMBDA;
	} else if (strcmp(value, "ARROW") == 0) {
		return ExpressionType::ARROW;
	} else {
		return ExpressionType::INVALID;
	}
}

string ExpressionTypeToOperator(ExpressionType type) {
	switch (type) {
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
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "IS DISTINCT FROM";
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return "IS NOT DISTINCT FROM";
	case ExpressionType::CONJUNCTION_AND:
		return "AND";
	case ExpressionType::CONJUNCTION_OR:
		return "OR";
	default:
		return "";
	}
}

ExpressionType NegateComparisonExpression(ExpressionType type) {
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
		throw InternalException("Unsupported comparison type in negation");
	}
	return negated_type;
}

ExpressionType FlipComparisonExpression(ExpressionType type) {
	ExpressionType flipped_type = ExpressionType::INVALID;
	switch (type) {
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
	case ExpressionType::COMPARE_DISTINCT_FROM:
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
		throw InternalException("Unsupported comparison type in flip");
	}
	return flipped_type;
}

ExpressionType OperatorToExpressionType(const string &op) {
	if (op == "=" || op == "==") {
		return ExpressionType::COMPARE_EQUAL;
	} else if (op == "!=" || op == "<>") {
		return ExpressionType::COMPARE_NOTEQUAL;
	} else if (op == "<") {
		return ExpressionType::COMPARE_LESSTHAN;
	} else if (op == ">") {
		return ExpressionType::COMPARE_GREATERTHAN;
	} else if (op == "<=") {
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	} else if (op == ">=") {
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	}
	return ExpressionType::INVALID;
}

template <>
const char *EnumSerializer::EnumToString(ExpressionClass value) {
	switch (value) {
	case ExpressionClass::INVALID:
		return "INVALID";
	case ExpressionClass::AGGREGATE:
		return "AGGREGATE";
	case ExpressionClass::CASE:
		return "CASE";
	case ExpressionClass::CAST:
		return "CAST";
	case ExpressionClass::COLUMN_REF:
		return "COLUMN_REF";
	case ExpressionClass::COMPARISON:
		return "COMPARISON";
	case ExpressionClass::CONJUNCTION:
		return "CONJUNCTION";
	case ExpressionClass::CONSTANT:
		return "CONSTANT";
	case ExpressionClass::DEFAULT:
		return "DEFAULT";
	case ExpressionClass::FUNCTION:
		return "FUNCTION";
	case ExpressionClass::OPERATOR:
		return "OPERATOR";
	case ExpressionClass::STAR:
		return "STAR";
	case ExpressionClass::SUBQUERY:
		return "SUBQUERY";
	case ExpressionClass::WINDOW:
		return "WINDOW";
	case ExpressionClass::PARAMETER:
		return "PARAMETER";
	case ExpressionClass::COLLATE:
		return "COLLATE";
	case ExpressionClass::LAMBDA:
		return "LAMBDA";
	case ExpressionClass::POSITIONAL_REFERENCE:
		return "POSITIONAL_REFERENCE";
	case ExpressionClass::BETWEEN:
		return "BETWEEN";
	case ExpressionClass::BOUND_AGGREGATE:
		return "BOUND_AGGREGATE";
	case ExpressionClass::BOUND_CASE:
		return "BOUND_CASE";
	case ExpressionClass::BOUND_CAST:
		return "BOUND_CAST";
	case ExpressionClass::BOUND_COLUMN_REF:
		return "BOUND_COLUMN_REF";
	case ExpressionClass::BOUND_COMPARISON:
		return "BOUND_COMPARISON";
	case ExpressionClass::BOUND_CONJUNCTION:
		return "BOUND_CONJUNCTION";
	case ExpressionClass::BOUND_CONSTANT:
		return "BOUND_CONSTANT";
	case ExpressionClass::BOUND_DEFAULT:
		return "BOUND_DEFAULT";
	case ExpressionClass::BOUND_FUNCTION:
		return "BOUND_FUNCTION";
	case ExpressionClass::BOUND_OPERATOR:
		return "BOUND_OPERATOR";
	case ExpressionClass::BOUND_PARAMETER:
		return "BOUND_PARAMETER";
	case ExpressionClass::BOUND_REF:
		return "BOUND_REF";
	case ExpressionClass::BOUND_SUBQUERY:
		return "BOUND_SUBQUERY";
	case ExpressionClass::BOUND_WINDOW:
		return "BOUND_WINDOW";
	case ExpressionClass::BOUND_BETWEEN:
		return "BOUND_BETWEEN";
	case ExpressionClass::BOUND_UNNEST:
		return "BOUND_UNNEST";
	case ExpressionClass::BOUND_LAMBDA:
		return "BOUND_LAMBDA";
	case ExpressionClass::BOUND_EXPRESSION:
		return "BOUND_EXPRESSION";
	default:
		return "ExpressionClass::!!UNIMPLEMENTED_CASE!!";
	}
}

string ExpressionClassToString(ExpressionClass type) {
	return EnumSerializer::EnumToString(type);
}

template <>
ExpressionClass EnumSerializer::StringToEnum(const char *value) {
	if (strcmp(value, "INVALID") == 0) {
		return ExpressionClass::INVALID;
	} else if (strcmp(value, "AGGREGATE") == 0) {
		return ExpressionClass::AGGREGATE;
	} else if (strcmp(value, "CASE") == 0) {
		return ExpressionClass::CASE;
	} else if (strcmp(value, "CAST") == 0) {
		return ExpressionClass::CAST;
	} else if (strcmp(value, "COLUMN_REF") == 0) {
		return ExpressionClass::COLUMN_REF;
	} else if (strcmp(value, "COMPARISON") == 0) {
		return ExpressionClass::COMPARISON;
	} else if (strcmp(value, "CONJUNCTION") == 0) {
		return ExpressionClass::CONJUNCTION;
	} else if (strcmp(value, "CONSTANT") == 0) {
		return ExpressionClass::CONSTANT;
	} else if (strcmp(value, "DEFAULT") == 0) {
		return ExpressionClass::DEFAULT;
	} else if (strcmp(value, "FUNCTION") == 0) {
		return ExpressionClass::FUNCTION;
	} else if (strcmp(value, "OPERATOR") == 0) {
		return ExpressionClass::OPERATOR;
	} else if (strcmp(value, "STAR") == 0) {
		return ExpressionClass::STAR;
	} else if (strcmp(value, "SUBQUERY") == 0) {
		return ExpressionClass::SUBQUERY;
	} else if (strcmp(value, "WINDOW") == 0) {
		return ExpressionClass::WINDOW;
	} else if (strcmp(value, "PARAMETER") == 0) {
		return ExpressionClass::PARAMETER;
	} else if (strcmp(value, "COLLATE") == 0) {
		return ExpressionClass::COLLATE;
	} else if (strcmp(value, "LAMBDA") == 0) {
		return ExpressionClass::LAMBDA;
	} else if (strcmp(value, "POSITIONAL_REFERENCE") == 0) {
		return ExpressionClass::POSITIONAL_REFERENCE;
	} else if (strcmp(value, "BETWEEN") == 0) {
		return ExpressionClass::BETWEEN;
	} else if (strcmp(value, "BOUND_AGGREGATE") == 0) {
		return ExpressionClass::BOUND_AGGREGATE;
	} else if (strcmp(value, "BOUND_CASE") == 0) {
		return ExpressionClass::BOUND_CASE;
	} else if (strcmp(value, "BOUND_CAST") == 0) {
		return ExpressionClass::BOUND_CAST;
	} else if (strcmp(value, "BOUND_COLUMN_REF") == 0) {
		return ExpressionClass::BOUND_COLUMN_REF;
	} else if (strcmp(value, "BOUND_COMPARISON") == 0) {
		return ExpressionClass::BOUND_COMPARISON;
	} else if (strcmp(value, "BOUND_CONJUNCTION") == 0) {
		return ExpressionClass::BOUND_CONJUNCTION;
	} else if (strcmp(value, "BOUND_CONSTANT") == 0) {
		return ExpressionClass::BOUND_CONSTANT;
	} else if (strcmp(value, "BOUND_DEFAULT") == 0) {
		return ExpressionClass::BOUND_DEFAULT;
	} else if (strcmp(value, "BOUND_FUNCTION") == 0) {
		return ExpressionClass::BOUND_FUNCTION;
	} else if (strcmp(value, "BOUND_OPERATOR") == 0) {
		return ExpressionClass::BOUND_OPERATOR;
	} else if (strcmp(value, "BOUND_PARAMETER") == 0) {
		return ExpressionClass::BOUND_PARAMETER;
	} else if (strcmp(value, "BOUND_REF") == 0) {
		return ExpressionClass::BOUND_REF;
	} else if (strcmp(value, "BOUND_SUBQUERY") == 0) {
		return ExpressionClass::BOUND_SUBQUERY;
	} else if (strcmp(value, "BOUND_WINDOW") == 0) {
		return ExpressionClass::BOUND_WINDOW;
	} else if (strcmp(value, "BOUND_BETWEEN") == 0) {
		return ExpressionClass::BOUND_BETWEEN;
	} else if (strcmp(value, "BOUND_UNNEST") == 0) {
		return ExpressionClass::BOUND_UNNEST;
	} else if (strcmp(value, "BOUND_LAMBDA") == 0) {
		return ExpressionClass::BOUND_LAMBDA;
	} else if (strcmp(value, "BOUND_EXPRESSION") == 0) {
		return ExpressionClass::BOUND_EXPRESSION;
	} else {
		throw NotImplementedException("Unrecognized ExpressionClass value");
	}
}

} // namespace duckdb
