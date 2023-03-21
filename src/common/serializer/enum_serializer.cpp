#include "duckdb/common/serializer/enum_serializer.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/aggregate_handling.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"

#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"

namespace duckdb {

//-----------------------------------------------
// OrderType
//-----------------------------------------------
template <>
OrderType EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "INVALID")) {
		return OrderType::INVALID;
	} else if (StringUtil::Equals(value, "ORDER_DEFAULT")) {
		return OrderType::ORDER_DEFAULT;
	} else if (StringUtil::Equals(value, "ASCENDING")) {
		return OrderType::ASCENDING;
	} else if (StringUtil::Equals(value, "DESCENDING")) {
		return OrderType::DESCENDING;
	} else {
		throw NotImplementedException("FromString not implemented for enum value");
	}
}

template <>
const char *EnumSerializer::EnumToString(OrderType value) {
	switch (value) {
	case OrderType::INVALID:
		return "INVALID";
	case OrderType::ORDER_DEFAULT:
		return "ORDER_DEFAULT";
	case OrderType::ASCENDING:
		return "ASCENDING";
	case OrderType::DESCENDING:
		return "DESCENDING";
	default:
		throw NotImplementedException("ToString not implemented for enum value");
	}
}

//-----------------------------------------------
// OrderByNullType
//-----------------------------------------------
template <>
OrderByNullType EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "INVALID")) {
		return OrderByNullType::INVALID;
	} else if (StringUtil::Equals(value, "ORDER_DEFAULT")) {
		return OrderByNullType::ORDER_DEFAULT;
	} else if (StringUtil::Equals(value, "NULLS_FIRST")) {
		return OrderByNullType::NULLS_FIRST;
	} else if (StringUtil::Equals(value, "NULLS_LAST")) {
		return OrderByNullType::NULLS_LAST;
	} else {
		throw NotImplementedException("FromString not implemented for enum value");
	}
}

template <>
const char *EnumSerializer::EnumToString(OrderByNullType value) {
	switch (value) {
	case OrderByNullType::INVALID:
		return "INVALID";
	case OrderByNullType::ORDER_DEFAULT:
		return "ORDER_DEFAULT";
	case OrderByNullType::NULLS_FIRST:
		return "NULLS_FIRST";
	case OrderByNullType::NULLS_LAST:
		return "NULLS_LAST";
	default:
		throw NotImplementedException("ToString not implemented for enum value");
	}
}

//-----------------------------------------------
// ResultModifierType
//-----------------------------------------------
template <>
ResultModifierType EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "LIMIT_MODIFIER")) {
		return ResultModifierType::LIMIT_MODIFIER;
	} else if (StringUtil::Equals(value, "ORDER_MODIFIER")) {
		return ResultModifierType::ORDER_MODIFIER;
	} else if (StringUtil::Equals(value, "DISTINCT_MODIFIER")) {
		return ResultModifierType::DISTINCT_MODIFIER;
	} else if (StringUtil::Equals(value, "LIMIT_PERCENT_MODIFIER")) {
		return ResultModifierType::LIMIT_PERCENT_MODIFIER;
	} else {
		throw NotImplementedException("FromString not implement for enum value");
	}
}

template <>
const char *EnumSerializer::EnumToString(ResultModifierType value) {
	switch (value) {
	case ResultModifierType::LIMIT_MODIFIER:
		return "LIMIT_MODIFIER";
	case ResultModifierType::ORDER_MODIFIER:
		return "ORDER_MODIFIER";
	case ResultModifierType::DISTINCT_MODIFIER:
		return "DISTINCT_MODIFIER";
	case ResultModifierType::LIMIT_PERCENT_MODIFIER:
		return "LIMIT_PERCENT_MODIFIER";
	default:
		throw NotImplementedException("ToString not implemented for enum value");
	}
}

//-----------------------------------------------
// ExtraTypeInfoType
//-----------------------------------------------
template <>
ExtraTypeInfoType EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "INVALID_TYPE_INFO")) {
		return ExtraTypeInfoType::INVALID_TYPE_INFO;
	} else if (StringUtil::Equals(value, "GENERIC_TYPE_INFO")) {
		return ExtraTypeInfoType::GENERIC_TYPE_INFO;
	} else if (StringUtil::Equals(value, "DECIMAL_TYPE_INFO")) {
		return ExtraTypeInfoType::DECIMAL_TYPE_INFO;
	} else if (StringUtil::Equals(value, "STRING_TYPE_INFO")) {
		return ExtraTypeInfoType::STRING_TYPE_INFO;
	} else if (StringUtil::Equals(value, "LIST_TYPE_INFO")) {
		return ExtraTypeInfoType::LIST_TYPE_INFO;
	} else if (StringUtil::Equals(value, "STRUCT_TYPE_INFO")) {
		return ExtraTypeInfoType::STRUCT_TYPE_INFO;
	} else if (StringUtil::Equals(value, "ENUM_TYPE_INFO")) {
		return ExtraTypeInfoType::USER_TYPE_INFO;
	} else if (StringUtil::Equals(value, "USER_TYPE_INFO")) {
		return ExtraTypeInfoType::USER_TYPE_INFO;
	} else if (StringUtil::Equals(value, "AGGREGATE_STATE_TYPE_INFO")) {
		return ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO;
	} else {
		throw NotImplementedException("FromString not implemented for enum value");
	}
}

template <>
const char *EnumSerializer::EnumToString(ExtraTypeInfoType value) {
	switch (value) {
	case ExtraTypeInfoType::INVALID_TYPE_INFO:
		return "INVALID_TYPE_INFO";
	case ExtraTypeInfoType::GENERIC_TYPE_INFO:
		return "GENERIC_TYPE_INFO";
	case ExtraTypeInfoType::DECIMAL_TYPE_INFO:
		return "DECIMAL_TYPE_INFO";
	case ExtraTypeInfoType::STRING_TYPE_INFO:
		return "STRING_TYPE_INFO";
	case ExtraTypeInfoType::LIST_TYPE_INFO:
		return "LIST_TYPE_INFO";
	case ExtraTypeInfoType::STRUCT_TYPE_INFO:
		return "STRUCT_TYPE_INFO";
	case ExtraTypeInfoType::ENUM_TYPE_INFO:
		return "ENUM_TYPE_INFO";
	case ExtraTypeInfoType::USER_TYPE_INFO:
		return "USER_TYPE_INFO";
	case ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO:
		return "AGGREGATE_STATE_TYPE_INFO";
	default:
		throw NotImplementedException("ToString not implemented for enum value");
	}
}

//-----------------------------------------------
// TableReferenceType
//-----------------------------------------------
template <>
TableReferenceType EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "INVALID")) {
		return TableReferenceType::INVALID;
	} else if (StringUtil::Equals(value, "BASE_TABLE")) {
		return TableReferenceType::BASE_TABLE;
	} else if (StringUtil::Equals(value, "SUBQUERY")) {
		return TableReferenceType::SUBQUERY;
	} else if (StringUtil::Equals(value, "JOIN")) {
		return TableReferenceType::JOIN;
	} else if (StringUtil::Equals(value, "TABLE_FUNCTION")) {
		return TableReferenceType::TABLE_FUNCTION;
	} else if (StringUtil::Equals(value, "EXPRESSION_LIST")) {
		return TableReferenceType::EXPRESSION_LIST;
	} else if (StringUtil::Equals(value, "CTE")) {
		return TableReferenceType::CTE;
	} else if (StringUtil::Equals(value, "EMPTY")) {
		return TableReferenceType::EMPTY;
	} else {
		throw NotImplementedException("FromString not implemented for enum value");
	}
}

template <>
const char *EnumSerializer::EnumToString(TableReferenceType value) {
	switch (value) {
	case TableReferenceType::INVALID:
		return "INVALID";
	case TableReferenceType::BASE_TABLE:
		return "BASE_TABLE";
	case TableReferenceType::SUBQUERY:
		return "SUBQUERY";
	case TableReferenceType::JOIN:
		return "JOIN";
	case TableReferenceType::TABLE_FUNCTION:
		return "TABLE_FUNCTION";
	case TableReferenceType::EXPRESSION_LIST:
		return "EXPRESSION_LIST";
	case TableReferenceType::CTE:
		return "CTE";
	case TableReferenceType::EMPTY:
		return "EMPTY";
	default:
		throw NotImplementedException("ToString not implemented for enum value");
	}
}

//-----------------------------------------------
// JoinRefType
//-----------------------------------------------
template <>
JoinRefType EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "REGULAR")) {
		return JoinRefType::REGULAR;
	} else if (StringUtil::Equals(value, "NATURAL")) {
		return JoinRefType::NATURAL;
	} else if (StringUtil::Equals(value, "CROSS")) {
		return JoinRefType::CROSS;
	} else if (StringUtil::Equals(value, "POSITIONAL")) {
		return JoinRefType::POSITIONAL;
	} else {
		throw NotImplementedException("EnumSerializer::StringToEnum not implemented for enum value");
	}
}

template <>
const char *EnumSerializer::EnumToString(JoinRefType value) {
	switch (value) {
	case JoinRefType::REGULAR:
		return "REGULAR";
	case JoinRefType::NATURAL:
		return "NATURAL";
	case JoinRefType::CROSS:
		return "CROSS";
	case JoinRefType::POSITIONAL:
		return "POSITIONAL";
	default:
		throw NotImplementedException("ToString not implemented for enum value");
	}
}

//-----------------------------------------------
// JoinType
//-----------------------------------------------

template <>
JoinType EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "LEFT")) {
		return JoinType::LEFT;
	} else if (StringUtil::Equals(value, "RIGHT")) {
		return JoinType::RIGHT;
	} else if (StringUtil::Equals(value, "INNER")) {
		return JoinType::INNER;
	} else if (StringUtil::Equals(value, "FULL")) {
		return JoinType::OUTER;
	} else if (StringUtil::Equals(value, "SEMI")) {
		return JoinType::SEMI;
	} else if (StringUtil::Equals(value, "ANTI")) {
		return JoinType::ANTI;
	} else if (StringUtil::Equals(value, "SINGLE")) {
		return JoinType::SINGLE;
	} else if (StringUtil::Equals(value, "MARK")) {
		return JoinType::MARK;
	} else {
		throw NotImplementedException("EnumSerializer::StringToEnum not implemented for enum value");
	}
}

template <>
const char *EnumSerializer::EnumToString(JoinType value) {
	switch (value) {
	case JoinType::LEFT:
		return "LEFT";
	case JoinType::RIGHT:
		return "RIGHT";
	case JoinType::INNER:
		return "INNER";
	case JoinType::OUTER:
		return "FULL";
	case JoinType::SEMI:
		return "SEMI";
	case JoinType::ANTI:
		return "ANTI";
	case JoinType::SINGLE:
		return "SINGLE";
	case JoinType::MARK:
		return "MARK";
	case JoinType::INVALID:
		return "INVALID";
	}
	return "INVALID";
}

//-----------------------------------------------
// AggregateHandling
//-----------------------------------------------

template <>
AggregateHandling EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "STANDARD_HANDLING")) {
		return AggregateHandling::STANDARD_HANDLING;
	} else if (StringUtil::Equals(value, "NO_AGGREGATES_ALLOWED")) {
		return AggregateHandling::NO_AGGREGATES_ALLOWED;
	} else if (StringUtil::Equals(value, "FORCE_AGGREGATES")) {
		return AggregateHandling::FORCE_AGGREGATES;
	} else {
		throw NotImplementedException("EnumSerializer::StringToEnum not implemented for enum value");
	}
}

template <>
const char *EnumSerializer::EnumToString(AggregateHandling value) {
	switch (value) {
	case AggregateHandling::STANDARD_HANDLING:
		return "STANDARD_HANDLING";
	case AggregateHandling::NO_AGGREGATES_ALLOWED:
		return "NO_AGGREGATES_ALLOWED";
	case AggregateHandling::FORCE_AGGREGATES:
		return "FORCE_AGGREGATES";
	default:
		throw NotImplementedException("ToString not implemented for enum value");
	}
}

//-----------------------------------------------
// QueryNodeType
//-----------------------------------------------
template <>
QueryNodeType EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "SELECT_NODE")) {
		return QueryNodeType::SELECT_NODE;
	} else if (StringUtil::Equals(value, "SET_OPERATION_NODE")) {
		return QueryNodeType::SET_OPERATION_NODE;
	} else if (StringUtil::Equals(value, "BOUND_SUBQUERY_NODE")) {
		return QueryNodeType::BOUND_SUBQUERY_NODE;
	} else if (StringUtil::Equals(value, "RECURSIVE_CTE_NODE")) {
		return QueryNodeType::RECURSIVE_CTE_NODE;
	} else {
		throw NotImplementedException("EnumSerializer::StringToEnum not implemented for string value");
	}
}

template <>
const char *EnumSerializer::EnumToString(QueryNodeType value) {
	switch (value) {
	case QueryNodeType::SELECT_NODE:
		return "SELECT_NODE";
	case QueryNodeType::SET_OPERATION_NODE:
		return "SET_OPERATION_NODE";
	case QueryNodeType::BOUND_SUBQUERY_NODE:
		return "BOUND_SUBQUERY_NODE";
	case QueryNodeType::RECURSIVE_CTE_NODE:
		return "RECURSIVE_CTE_NODE";
	default:
		throw NotImplementedException("EnumSerializer::EnumToString not implemented for enum value");
	}
}

//-----------------------------------------------
// SetOperationType
//-----------------------------------------------
template <>
SetOperationType EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "NONE")) {
		return SetOperationType::NONE;
	} else if (StringUtil::Equals(value, "UNION")) {
		return SetOperationType::UNION;
	} else if (StringUtil::Equals(value, "EXCEPT")) {
		return SetOperationType::EXCEPT;
	} else if (StringUtil::Equals(value, "INTERSECT")) {
		return SetOperationType::INTERSECT;
	} else if (StringUtil::Equals(value, "UNION_BY_NAME")) {
		return SetOperationType::UNION_BY_NAME;
	} else {
		throw NotImplementedException("EnumSerializer::StringToEnum not implemented for enum value");
	}
}

template <>
const char *EnumSerializer::EnumToString(SetOperationType value) {
	switch (value) {
	case SetOperationType::NONE:
		return "NONE";
	case SetOperationType::UNION:
		return "UNION";
	case SetOperationType::EXCEPT:
		return "EXCEPT";
	case SetOperationType::INTERSECT:
		return "INTERSECT";
	case SetOperationType::UNION_BY_NAME:
		return "UNION_BY_NAME";
	default:
		throw NotImplementedException("EnumSerializer::EnumToString not implemented for enum value");
	}
}

//-----------------------------------------------
// WindowBoundary
//-----------------------------------------------
template <>
WindowBoundary EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "INVALID")) {
		return WindowBoundary::INVALID;
	} else if (StringUtil::Equals(value, "UNBOUNDED_PRECEDING")) {
		return WindowBoundary::UNBOUNDED_PRECEDING;
	} else if (StringUtil::Equals(value, "UNBOUNDED_FOLLOWING")) {
		return WindowBoundary::UNBOUNDED_FOLLOWING;
	} else if (StringUtil::Equals(value, "CURRENT_ROW_RANGE")) {
		return WindowBoundary::CURRENT_ROW_RANGE;
	} else if (StringUtil::Equals(value, "CURRENT_ROW_ROWS")) {
		return WindowBoundary::CURRENT_ROW_ROWS;
	} else if (StringUtil::Equals(value, "EXPR_PRECEDING_ROWS")) {
		return WindowBoundary::EXPR_PRECEDING_ROWS;
	} else if (StringUtil::Equals(value, "EXPR_FOLLOWING_ROWS")) {
		return WindowBoundary::EXPR_FOLLOWING_ROWS;
	} else if (StringUtil::Equals(value, "EXPR_PRECEDING_RANGE")) {
		return WindowBoundary::EXPR_PRECEDING_RANGE;
	} else if (StringUtil::Equals(value, "EXPR_FOLLOWING_RANGE")) {
		return WindowBoundary::EXPR_FOLLOWING_RANGE;
	} else {
		throw NotImplementedException("FromString not implemented for enum value");
	}
}

template <>
const char *EnumSerializer::EnumToString(WindowBoundary value) {
	switch (value) {
	case WindowBoundary::INVALID:
		return "INVALID";
	case WindowBoundary::UNBOUNDED_PRECEDING:
		return "UNBOUNDED_PRECEDING";
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		return "UNBOUNDED_FOLLOWING";
	case WindowBoundary::CURRENT_ROW_RANGE:
		return "CURRENT_ROW_RANGE";
	case WindowBoundary::CURRENT_ROW_ROWS:
		return "CURRENT_ROW_ROWS";
	case WindowBoundary::EXPR_PRECEDING_ROWS:
		return "EXPR_PRECEDING_ROWS";
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		return "EXPR_FOLLOWING_ROWS";
	case WindowBoundary::EXPR_PRECEDING_RANGE:
		return "EXPR_PRECEDING_RANGE";
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		return "EXPR_FOLLOWING_RANGE";
	default:
		throw NotImplementedException("ToString not implemented for enum value");
	}
}

//-----------------------------------------------
// SubqueryType
//-----------------------------------------------
template <>
SubqueryType EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "INVALID")) {
		return SubqueryType::INVALID;
	} else if (StringUtil::Equals(value, "SCALAR")) {
		return SubqueryType::SCALAR;
	} else if (StringUtil::Equals(value, "EXISTS")) {
		return SubqueryType::EXISTS;
	} else if (StringUtil::Equals(value, "NOT_EXISTS")) {
		return SubqueryType::NOT_EXISTS;
	} else if (StringUtil::Equals(value, "ANY")) {
		return SubqueryType::ANY;
	} else {
		throw NotImplementedException("EnumSerializer::StringToEnum not implemented for enum value");
	}
}

template <>
const char *EnumSerializer::EnumToString(SubqueryType value) {
	switch (value) {
	case SubqueryType::INVALID:
		return "INVALID";
	case SubqueryType::SCALAR:
		return "SCALAR";
	case SubqueryType::EXISTS:
		return "EXISTS";
	case SubqueryType::NOT_EXISTS:
		return "NOT_EXISTS";
	case SubqueryType::ANY:
		return "ANY";
	default:
		throw NotImplementedException("EnumSerializer::EnumToString not implemented for enum value");
	}
}

//-----------------------------------------------
// ExpressionType
//-----------------------------------------------
template <>
ExpressionType EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "CAST")) {
		return ExpressionType::OPERATOR_CAST;
	} else if (StringUtil::Equals(value, "NOT")) {
		return ExpressionType::OPERATOR_NOT;
	} else if (StringUtil::Equals(value, "IS_NULL")) {
		return ExpressionType::OPERATOR_IS_NULL;
	} else if (StringUtil::Equals(value, "IS_NOT_NULL")) {
		return ExpressionType::OPERATOR_IS_NOT_NULL;
	} else if (StringUtil::Equals(value, "EQUAL")) {
		return ExpressionType::COMPARE_EQUAL;
	} else if (StringUtil::Equals(value, "NOTEQUAL")) {
		return ExpressionType::COMPARE_NOTEQUAL;
	} else if (StringUtil::Equals(value, "LESSTHAN")) {
		return ExpressionType::COMPARE_LESSTHAN;
	} else if (StringUtil::Equals(value, "GREATERTHAN")) {
		return ExpressionType::COMPARE_GREATERTHAN;
	} else if (StringUtil::Equals(value, "LESSTHANOREQUALTO")) {
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	} else if (StringUtil::Equals(value, "GREATERTHANOREQUALTO")) {
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	} else if (StringUtil::Equals(value, "IN")) {
		return ExpressionType::COMPARE_IN;
	} else if (StringUtil::Equals(value, "DISTINCT_FROM")) {
		return ExpressionType::COMPARE_DISTINCT_FROM;
	} else if (StringUtil::Equals(value, "NOT_DISTINCT_FROM")) {
		return ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	} else if (StringUtil::Equals(value, "AND")) {
		return ExpressionType::CONJUNCTION_AND;
	} else if (StringUtil::Equals(value, "OR")) {
		return ExpressionType::CONJUNCTION_OR;
	} else if (StringUtil::Equals(value, "CONSTANT")) {
		return ExpressionType::VALUE_CONSTANT;
	} else if (StringUtil::Equals(value, "PARAMETER")) {
		return ExpressionType::VALUE_PARAMETER;
	} else if (StringUtil::Equals(value, "TUPLE")) {
		return ExpressionType::VALUE_TUPLE;
	} else if (StringUtil::Equals(value, "TUPLE_ADDRESS")) {
		return ExpressionType::VALUE_TUPLE_ADDRESS;
	} else if (StringUtil::Equals(value, "NULL")) {
		return ExpressionType::VALUE_NULL;
	} else if (StringUtil::Equals(value, "VECTOR")) {
		return ExpressionType::VALUE_VECTOR;
	} else if (StringUtil::Equals(value, "SCALAR")) {
		return ExpressionType::VALUE_SCALAR;
	} else if (StringUtil::Equals(value, "AGGREGATE")) {
		return ExpressionType::AGGREGATE;
	} else if (StringUtil::Equals(value, "WINDOW_AGGREGATE")) {
		return ExpressionType::WINDOW_AGGREGATE;
	} else if (StringUtil::Equals(value, "RANK")) {
		return ExpressionType::WINDOW_RANK;
	} else if (StringUtil::Equals(value, "RANK_DENSE")) {
		return ExpressionType::WINDOW_RANK_DENSE;
	} else if (StringUtil::Equals(value, "PERCENT_RANK")) {
		return ExpressionType::WINDOW_PERCENT_RANK;
	} else if (StringUtil::Equals(value, "ROW_NUMBER")) {
		return ExpressionType::WINDOW_ROW_NUMBER;
	} else if (StringUtil::Equals(value, "FIRST_VALUE")) {
		return ExpressionType::WINDOW_FIRST_VALUE;
	} else if (StringUtil::Equals(value, "LAST_VALUE")) {
		return ExpressionType::WINDOW_LAST_VALUE;
	} else if (StringUtil::Equals(value, "NTH_VALUE")) {
		return ExpressionType::WINDOW_NTH_VALUE;
	} else if (StringUtil::Equals(value, "CUME_DIST")) {
		return ExpressionType::WINDOW_CUME_DIST;
	} else if (StringUtil::Equals(value, "LEAD")) {
		return ExpressionType::WINDOW_LEAD;
	} else if (StringUtil::Equals(value, "LAG")) {
		return ExpressionType::WINDOW_LAG;
	} else if (StringUtil::Equals(value, "NTILE")) {
		return ExpressionType::WINDOW_NTILE;
	} else if (StringUtil::Equals(value, "FUNCTION")) {
		return ExpressionType::FUNCTION;
	} else if (StringUtil::Equals(value, "CASE")) {
		return ExpressionType::CASE_EXPR;
	} else if (StringUtil::Equals(value, "NULLIF")) {
		return ExpressionType::OPERATOR_NULLIF;
	} else if (StringUtil::Equals(value, "COALESCE")) {
		return ExpressionType::OPERATOR_COALESCE;
	} else if (StringUtil::Equals(value, "ARRAY_EXTRACT")) {
		return ExpressionType::ARRAY_EXTRACT;
	} else if (StringUtil::Equals(value, "ARRAY_SLICE")) {
		return ExpressionType::ARRAY_SLICE;
	} else if (StringUtil::Equals(value, "STRUCT_EXTRACT")) {
		return ExpressionType::STRUCT_EXTRACT;
	} else if (StringUtil::Equals(value, "SUBQUERY")) {
		return ExpressionType::SUBQUERY;
	} else if (StringUtil::Equals(value, "STAR")) {
		return ExpressionType::STAR;
	} else if (StringUtil::Equals(value, "PLACEHOLDER")) {
		return ExpressionType::PLACEHOLDER;
	} else if (StringUtil::Equals(value, "COLUMN_REF")) {
		return ExpressionType::COLUMN_REF;
	} else if (StringUtil::Equals(value, "FUNCTION_REF")) {
		return ExpressionType::FUNCTION_REF;
	} else if (StringUtil::Equals(value, "TABLE_REF")) {
		return ExpressionType::TABLE_REF;
	} else if (StringUtil::Equals(value, "CAST")) {
		return ExpressionType::CAST;
	} else if (StringUtil::Equals(value, "COMPARE_NOT_IN")) {
		return ExpressionType::COMPARE_NOT_IN;
	} else if (StringUtil::Equals(value, "COMPARE_BETWEEN")) {
		return ExpressionType::COMPARE_BETWEEN;
	} else if (StringUtil::Equals(value, "COMPARE_NOT_BETWEEN")) {
		return ExpressionType::COMPARE_NOT_BETWEEN;
	} else if (StringUtil::Equals(value, "VALUE_DEFAULT")) {
		return ExpressionType::VALUE_DEFAULT;
	} else if (StringUtil::Equals(value, "BOUND_REF")) {
		return ExpressionType::BOUND_REF;
	} else if (StringUtil::Equals(value, "BOUND_COLUMN_REF")) {
		return ExpressionType::BOUND_COLUMN_REF;
	} else if (StringUtil::Equals(value, "BOUND_FUNCTION")) {
		return ExpressionType::BOUND_FUNCTION;
	} else if (StringUtil::Equals(value, "BOUND_AGGREGATE")) {
		return ExpressionType::BOUND_AGGREGATE;
	} else if (StringUtil::Equals(value, "GROUPING")) {
		return ExpressionType::GROUPING_FUNCTION;
	} else if (StringUtil::Equals(value, "ARRAY_CONSTRUCTOR")) {
		return ExpressionType::ARRAY_CONSTRUCTOR;
	} else if (StringUtil::Equals(value, "TABLE_STAR")) {
		return ExpressionType::TABLE_STAR;
	} else if (StringUtil::Equals(value, "BOUND_UNNEST")) {
		return ExpressionType::BOUND_UNNEST;
	} else if (StringUtil::Equals(value, "COLLATE")) {
		return ExpressionType::COLLATE;
	} else if (StringUtil::Equals(value, "POSITIONAL_REFERENCE")) {
		return ExpressionType::POSITIONAL_REFERENCE;
	} else if (StringUtil::Equals(value, "BOUND_LAMBDA_REF")) {
		return ExpressionType::BOUND_LAMBDA_REF;
	} else if (StringUtil::Equals(value, "LAMBDA")) {
		return ExpressionType::LAMBDA;
	} else if (StringUtil::Equals(value, "ARROW")) {
		return ExpressionType::ARROW;
	} else {
		return ExpressionType::INVALID;
	}
}

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

//-----------------------------------------------
// ExpressionClass
//-----------------------------------------------
template <>
ExpressionClass EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "INVALID")) {
		return ExpressionClass::INVALID;
	} else if (StringUtil::Equals(value, "AGGREGATE")) {
		return ExpressionClass::AGGREGATE;
	} else if (StringUtil::Equals(value, "CASE")) {
		return ExpressionClass::CASE;
	} else if (StringUtil::Equals(value, "CAST")) {
		return ExpressionClass::CAST;
	} else if (StringUtil::Equals(value, "COLUMN_REF")) {
		return ExpressionClass::COLUMN_REF;
	} else if (StringUtil::Equals(value, "COMPARISON")) {
		return ExpressionClass::COMPARISON;
	} else if (StringUtil::Equals(value, "CONJUNCTION")) {
		return ExpressionClass::CONJUNCTION;
	} else if (StringUtil::Equals(value, "CONSTANT")) {
		return ExpressionClass::CONSTANT;
	} else if (StringUtil::Equals(value, "DEFAULT")) {
		return ExpressionClass::DEFAULT;
	} else if (StringUtil::Equals(value, "FUNCTION")) {
		return ExpressionClass::FUNCTION;
	} else if (StringUtil::Equals(value, "OPERATOR")) {
		return ExpressionClass::OPERATOR;
	} else if (StringUtil::Equals(value, "STAR")) {
		return ExpressionClass::STAR;
	} else if (StringUtil::Equals(value, "SUBQUERY")) {
		return ExpressionClass::SUBQUERY;
	} else if (StringUtil::Equals(value, "WINDOW")) {
		return ExpressionClass::WINDOW;
	} else if (StringUtil::Equals(value, "PARAMETER")) {
		return ExpressionClass::PARAMETER;
	} else if (StringUtil::Equals(value, "COLLATE")) {
		return ExpressionClass::COLLATE;
	} else if (StringUtil::Equals(value, "LAMBDA")) {
		return ExpressionClass::LAMBDA;
	} else if (StringUtil::Equals(value, "POSITIONAL_REFERENCE")) {
		return ExpressionClass::POSITIONAL_REFERENCE;
	} else if (StringUtil::Equals(value, "BETWEEN")) {
		return ExpressionClass::BETWEEN;
	} else if (StringUtil::Equals(value, "BOUND_AGGREGATE")) {
		return ExpressionClass::BOUND_AGGREGATE;
	} else if (StringUtil::Equals(value, "BOUND_CASE")) {
		return ExpressionClass::BOUND_CASE;
	} else if (StringUtil::Equals(value, "BOUND_CAST")) {
		return ExpressionClass::BOUND_CAST;
	} else if (StringUtil::Equals(value, "BOUND_COLUMN_REF")) {
		return ExpressionClass::BOUND_COLUMN_REF;
	} else if (StringUtil::Equals(value, "BOUND_COMPARISON")) {
		return ExpressionClass::BOUND_COMPARISON;
	} else if (StringUtil::Equals(value, "BOUND_CONJUNCTION")) {
		return ExpressionClass::BOUND_CONJUNCTION;
	} else if (StringUtil::Equals(value, "BOUND_CONSTANT")) {
		return ExpressionClass::BOUND_CONSTANT;
	} else if (StringUtil::Equals(value, "BOUND_DEFAULT")) {
		return ExpressionClass::BOUND_DEFAULT;
	} else if (StringUtil::Equals(value, "BOUND_FUNCTION")) {
		return ExpressionClass::BOUND_FUNCTION;
	} else if (StringUtil::Equals(value, "BOUND_OPERATOR")) {
		return ExpressionClass::BOUND_OPERATOR;
	} else if (StringUtil::Equals(value, "BOUND_PARAMETER")) {
		return ExpressionClass::BOUND_PARAMETER;
	} else if (StringUtil::Equals(value, "BOUND_REF")) {
		return ExpressionClass::BOUND_REF;
	} else if (StringUtil::Equals(value, "BOUND_SUBQUERY")) {
		return ExpressionClass::BOUND_SUBQUERY;
	} else if (StringUtil::Equals(value, "BOUND_WINDOW")) {
		return ExpressionClass::BOUND_WINDOW;
	} else if (StringUtil::Equals(value, "BOUND_BETWEEN")) {
		return ExpressionClass::BOUND_BETWEEN;
	} else if (StringUtil::Equals(value, "BOUND_UNNEST")) {
		return ExpressionClass::BOUND_UNNEST;
	} else if (StringUtil::Equals(value, "BOUND_LAMBDA")) {
		return ExpressionClass::BOUND_LAMBDA;
	} else if (StringUtil::Equals(value, "BOUND_EXPRESSION")) {
		return ExpressionClass::BOUND_EXPRESSION;
	} else {
		throw NotImplementedException("Unrecognized ExpressionClass value");
	}
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

//-----------------------------------------------
// SampleMethod
//-----------------------------------------------
template <>
SampleMethod EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "System")) {
		return SampleMethod::SYSTEM_SAMPLE;
	} else if (StringUtil::Equals(value, "Bernoulli")) {
		return SampleMethod::BERNOULLI_SAMPLE;
	} else if (StringUtil::Equals(value, "Reservoir")) {
		return SampleMethod::RESERVOIR_SAMPLE;
	} else {
		throw NotImplementedException("Unrecognized sample method type \"%s\"", value);
	}
}

template <>
const char *EnumSerializer::EnumToString(SampleMethod value) {
	switch (value) {
	case SampleMethod::SYSTEM_SAMPLE:
		return "System";
	case SampleMethod::BERNOULLI_SAMPLE:
		return "Bernoulli";
	case SampleMethod::RESERVOIR_SAMPLE:
		return "Reservoir";
	default:
		return "Unknown";
	}
}

//-----------------------------------------------
// LogicalType
//-----------------------------------------------
template <>
LogicalTypeId EnumSerializer::StringToEnum(const char *value) {
	if (StringUtil::Equals(value, "BOOLEAN")) {
		return LogicalTypeId::BOOLEAN;
	} else if (StringUtil::Equals(value, "TINYINT")) {
		return LogicalTypeId::TINYINT;
	} else if (StringUtil::Equals(value, "SMALLINT")) {
		return LogicalTypeId::SMALLINT;
	} else if (StringUtil::Equals(value, "INTEGER")) {
		return LogicalTypeId::INTEGER;
	} else if (StringUtil::Equals(value, "BIGINT")) {
		return LogicalTypeId::BIGINT;
	} else if (StringUtil::Equals(value, "HUGEINT")) {
		return LogicalTypeId::HUGEINT;
	} else if (StringUtil::Equals(value, "UUID")) {
		return LogicalTypeId::UUID;
	} else if (StringUtil::Equals(value, "UTINYINT")) {
		return LogicalTypeId::UTINYINT;
	} else if (StringUtil::Equals(value, "USMALLINT")) {
		return LogicalTypeId::USMALLINT;
	} else if (StringUtil::Equals(value, "UINTEGER")) {
		return LogicalTypeId::UINTEGER;
	} else if (StringUtil::Equals(value, "UBIGINT")) {
		return LogicalTypeId::UBIGINT;
	} else if (StringUtil::Equals(value, "DATE")) {
		return LogicalTypeId::DATE;
	} else if (StringUtil::Equals(value, "TIME")) {
		return LogicalTypeId::TIME;
	} else if (StringUtil::Equals(value, "TIMESTAMP")) {
		return LogicalTypeId::TIMESTAMP;
	} else if (StringUtil::Equals(value, "TIMESTAMP_MS")) {
		return LogicalTypeId::TIMESTAMP_MS;
	} else if (StringUtil::Equals(value, "TIMESTAMP_NS")) {
		return LogicalTypeId::TIMESTAMP_NS;
	} else if (StringUtil::Equals(value, "TIMESTAMP_S")) {
		return LogicalTypeId::TIMESTAMP_SEC;
	} else if (StringUtil::Equals(value, "TIMESTAMP WITH TIME ZONE")) {
		return LogicalTypeId::TIMESTAMP_TZ;
	} else if (StringUtil::Equals(value, "TIME WITH TIME ZONE")) {
		return LogicalTypeId::TIME_TZ;
	} else if (StringUtil::Equals(value, "FLOAT")) {
		return LogicalTypeId::FLOAT;
	} else if (StringUtil::Equals(value, "DOUBLE")) {
		return LogicalTypeId::DOUBLE;
	} else if (StringUtil::Equals(value, "DECIMAL")) {
		return LogicalTypeId::DECIMAL;
	} else if (StringUtil::Equals(value, "VARCHAR")) {
		return LogicalTypeId::VARCHAR;
	} else if (StringUtil::Equals(value, "BLOB")) {
		return LogicalTypeId::BLOB;
	} else if (StringUtil::Equals(value, "CHAR")) {
		return LogicalTypeId::CHAR;
	} else if (StringUtil::Equals(value, "INTERVAL")) {
		return LogicalTypeId::INTERVAL;
	} else if (StringUtil::Equals(value, "NULL")) {
		return LogicalTypeId::SQLNULL;
	} else if (StringUtil::Equals(value, "ANY")) {
		return LogicalTypeId::ANY;
	} else if (StringUtil::Equals(value, "VALIDITY")) {
		return LogicalTypeId::VALIDITY;
	} else if (StringUtil::Equals(value, "STRUCT")) {
		return LogicalTypeId::STRUCT;
	} else if (StringUtil::Equals(value, "LIST")) {
		return LogicalTypeId::LIST;
	} else if (StringUtil::Equals(value, "MAP")) {
		return LogicalTypeId::MAP;
	} else if (StringUtil::Equals(value, "POINTER")) {
		return LogicalTypeId::POINTER;
	} else if (StringUtil::Equals(value, "TABLE")) {
		return LogicalTypeId::TABLE;
	} else if (StringUtil::Equals(value, "LAMBDA")) {
		return LogicalTypeId::LAMBDA;
	} else if (StringUtil::Equals(value, "INVALID")) {
		return LogicalTypeId::INVALID;
	} else if (StringUtil::Equals(value, "UNION")) {
		return LogicalTypeId::UNION;
	} else if (StringUtil::Equals(value, "UNKNOWN")) {
		return LogicalTypeId::UNKNOWN;
	} else if (StringUtil::Equals(value, "ENUM")) {
		return LogicalTypeId::ENUM;
	} else if (StringUtil::Equals(value, "AGGREGATE_STATE")) {
		return LogicalTypeId::AGGREGATE_STATE;
	} else if (StringUtil::Equals(value, "USER")) {
		return LogicalTypeId::USER;
	} else if (StringUtil::Equals(value, "BIT")) {
		return LogicalTypeId::BIT;
	} else {
		throw NotImplementedException(
		    "Unrecognized type LogicalTypeId in EnumSerializer::EnumSerializer::StringToEnum");
	}
}

template <>
const char *EnumSerializer::EnumToString(LogicalTypeId value) {
	switch (value) {
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
		return "TINYINT";
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
		return "BIGINT";
	case LogicalTypeId::HUGEINT:
		return "HUGEINT";
	case LogicalTypeId::UUID:
		return "UUID";
	case LogicalTypeId::UTINYINT:
		return "UTINYINT";
	case LogicalTypeId::USMALLINT:
		return "USMALLINT";
	case LogicalTypeId::UINTEGER:
		return "UINTEGER";
	case LogicalTypeId::UBIGINT:
		return "UBIGINT";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIME:
		return "TIME";
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP";
	case LogicalTypeId::TIMESTAMP_MS:
		return "TIMESTAMP_MS";
	case LogicalTypeId::TIMESTAMP_NS:
		return "TIMESTAMP_NS";
	case LogicalTypeId::TIMESTAMP_SEC:
		return "TIMESTAMP_S";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMP WITH TIME ZONE";
	case LogicalTypeId::TIME_TZ:
		return "TIME WITH TIME ZONE";
	case LogicalTypeId::FLOAT:
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::DECIMAL:
		return "DECIMAL";
	case LogicalTypeId::VARCHAR:
		return "VARCHAR";
	case LogicalTypeId::BLOB:
		return "BLOB";
	case LogicalTypeId::CHAR:
		return "CHAR";
	case LogicalTypeId::INTERVAL:
		return "INTERVAL";
	case LogicalTypeId::SQLNULL:
		return "NULL";
	case LogicalTypeId::ANY:
		return "ANY";
	case LogicalTypeId::VALIDITY:
		return "VALIDITY";
	case LogicalTypeId::STRUCT:
		return "STRUCT";
	case LogicalTypeId::LIST:
		return "LIST";
	case LogicalTypeId::MAP:
		return "MAP";
	case LogicalTypeId::POINTER:
		return "POINTER";
	case LogicalTypeId::TABLE:
		return "TABLE";
	case LogicalTypeId::LAMBDA:
		return "LAMBDA";
	case LogicalTypeId::INVALID:
		return "INVALID";
	case LogicalTypeId::UNION:
		return "UNION";
	case LogicalTypeId::UNKNOWN:
		return "UNKNOWN";
	case LogicalTypeId::ENUM:
		return "ENUM";
	case LogicalTypeId::AGGREGATE_STATE:
		return "AGGREGATE_STATE";
	case LogicalTypeId::USER:
		return "USER";
	case LogicalTypeId::BIT:
		return "BIT";
	}
	return "UNDEFINED";
}

} // namespace duckdb
