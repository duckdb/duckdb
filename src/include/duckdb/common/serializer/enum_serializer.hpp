#pragma once
#include <stdint.h>

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

namespace EnumSerializer {
// String -> Enum
template <class T>
T StringToEnum(const char *value) = delete;

// Enum -> String
template <class T>
const char *EnumToString(T value) = delete;

//-----------------------------------------------
// OrderType
//-----------------------------------------------
template <>
OrderType StringToEnum(const char *value) {
	if (strcmp(value, "INVALID") == 0) {
		return OrderType::INVALID;
	} else if (strcmp(value, "ORDER_DEFAULT") == 0) {
		return OrderType::ORDER_DEFAULT;
	} else if (strcmp(value, "ASCENDING") == 0) {
		return OrderType::ASCENDING;
	} else if (strcmp(value, "DESCENDING") == 0) {
		return OrderType::DESCENDING;
	} else {
		throw NotImplementedException("FromString not implemented for enum value");
	}
}

template <>
const char *EnumToString(OrderType value) {
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
OrderByNullType StringToEnum(const char *value) {
	if (strcmp(value, "INVALID") == 0) {
		return OrderByNullType::INVALID;
	} else if (strcmp(value, "ORDER_DEFAULT") == 0) {
		return OrderByNullType::ORDER_DEFAULT;
	} else if (strcmp(value, "NULLS_FIRST") == 0) {
		return OrderByNullType::NULLS_FIRST;
	} else if (strcmp(value, "NULLS_LAST") == 0) {
		return OrderByNullType::NULLS_LAST;
	} else {
		throw NotImplementedException("FromString not implemented for enum value");
	}
}

template <>
const char *EnumToString(OrderByNullType value) {
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
ResultModifierType StringToEnum(const char *value) {
	if (strcmp(value, "LIMIT_MODIFIER") == 0) {
		return ResultModifierType::LIMIT_MODIFIER;
	} else if (strcmp(value, "ORDER_MODIFIER") == 0) {
		return ResultModifierType::ORDER_MODIFIER;
	} else if (strcmp(value, "DISTINCT_MODIFIER") == 0) {
		return ResultModifierType::DISTINCT_MODIFIER;
	} else if (strcmp(value, "LIMIT_PERCENT_MODIFIER") == 0) {
		return ResultModifierType::LIMIT_PERCENT_MODIFIER;
	} else {
		throw NotImplementedException("FromString not implement for enum value");
	}
}

template <>
const char *EnumToString(ResultModifierType value) {
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
ExtraTypeInfoType StringToEnum(const char *value) {
	if (strcmp(value, "INVALID_TYPE_INFO") == 0) {
		return ExtraTypeInfoType::INVALID_TYPE_INFO;
	} else if (strcmp(value, "GENERIC_TYPE_INFO") == 0) {
		return ExtraTypeInfoType::GENERIC_TYPE_INFO;
	} else if (strcmp(value, "DECIMAL_TYPE_INFO") == 0) {
		return ExtraTypeInfoType::DECIMAL_TYPE_INFO;
	} else if (strcmp(value, "STRING_TYPE_INFO") == 0) {
		return ExtraTypeInfoType::STRING_TYPE_INFO;
	} else if (strcmp(value, "LIST_TYPE_INFO") == 0) {
		return ExtraTypeInfoType::LIST_TYPE_INFO;
	} else if (strcmp(value, "STRUCT_TYPE_INFO") == 0) {
		return ExtraTypeInfoType::STRUCT_TYPE_INFO;
	} else if (strcmp(value, "ENUM_TYPE_INFO") == 0) {
		return ExtraTypeInfoType::USER_TYPE_INFO;
	} else if (strcmp(value, "USER_TYPE_INFO") == 0) {
		return ExtraTypeInfoType::USER_TYPE_INFO;
	} else if (strcmp(value, "AGGREGATE_STATE_TYPE_INFO") == 0) {
		return ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO;
	} else {
		throw NotImplementedException("FromString not implemented for enum value");
	}
}

template <>
const char *EnumToString(ExtraTypeInfoType value) {
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
TableReferenceType StringToEnum(const char *value) {
	if (strcmp(value, "INVALID") == 0) {
		return TableReferenceType::INVALID;
	} else if (strcmp(value, "BASE_TABLE") == 0) {
		return TableReferenceType::BASE_TABLE;
	} else if (strcmp(value, "SUBQUERY") == 0) {
		return TableReferenceType::SUBQUERY;
	} else if (strcmp(value, "JOIN") == 0) {
		return TableReferenceType::JOIN;
	} else if (strcmp(value, "TABLE_FUNCTION") == 0) {
		return TableReferenceType::TABLE_FUNCTION;
	} else if (strcmp(value, "EXPRESSION_LIST") == 0) {
		return TableReferenceType::EXPRESSION_LIST;
	} else if (strcmp(value, "CTE") == 0) {
		return TableReferenceType::CTE;
	} else if (strcmp(value, "EMPTY") == 0) {
		return TableReferenceType::EMPTY;
	} else {
		throw NotImplementedException("FromString not implemented for enum value");
	}
}

template <>
const char *EnumToString(TableReferenceType value) {
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
JoinRefType StringToEnum(const char *value) {
	if (strcmp(value, "REGULAR") == 0) {
		return JoinRefType::REGULAR;
	} else if (strcmp(value, "NATURAL") == 0) {
		return JoinRefType::NATURAL;
	} else if (strcmp(value, "CROSS") == 0) {
		return JoinRefType::CROSS;
	} else if (strcmp(value, "POSITIONAL") == 0) {
		return JoinRefType::POSITIONAL;
	} else {
		throw NotImplementedException("StringToEnum not implemented for enum value");
	}
}

template <>
const char *EnumToString(JoinRefType value) {
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
JoinType StringToEnum(const char *value) {
	if (strcmp(value, "LEFT") == 0) {
		return JoinType::LEFT;
	} else if (strcmp(value, "RIGHT") == 0) {
		return JoinType::RIGHT;
	} else if (strcmp(value, "INNER") == 0) {
		return JoinType::INNER;
	} else if (strcmp(value, "FULL") == 0) {
		return JoinType::OUTER;
	} else if (strcmp(value, "SEMI") == 0) {
		return JoinType::SEMI;
	} else if (strcmp(value, "ANTI") == 0) {
		return JoinType::ANTI;
	} else if (strcmp(value, "SINGLE") == 0) {
		return JoinType::SINGLE;
	} else if (strcmp(value, "MARK") == 0) {
		return JoinType::MARK;
	} else {
		throw NotImplementedException("StringToEnum not implemented for enum value");
	}
}

template <>
const char *EnumToString(JoinType value) {
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
AggregateHandling StringToEnum(const char *value) {
	if (strcmp(value, "STANDARD_HANDLING") == 0) {
		return AggregateHandling::STANDARD_HANDLING;
	} else if (strcmp(value, "NO_AGGREGATES_ALLOWED") == 0) {
		return AggregateHandling::NO_AGGREGATES_ALLOWED;
	} else if (strcmp(value, "FORCE_AGGREGATES") == 0) {
		return AggregateHandling::FORCE_AGGREGATES;
	} else {
		throw NotImplementedException("StringToEnum not implemented for enum value");
	}
}

template <>
const char *EnumToString(AggregateHandling value) {
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
QueryNodeType StringToEnum(const char *value) {
	if (strcmp(value, "SELECT_NODE") == 0) {
		return QueryNodeType::SELECT_NODE;
	} else if (strcmp(value, "SET_OPERATION_NODE") == 0) {
		return QueryNodeType::SET_OPERATION_NODE;
	} else if (strcmp(value, "BOUND_SUBQUERY_NODE") == 0) {
		return QueryNodeType::BOUND_SUBQUERY_NODE;
	} else if (strcmp(value, "RECURSIVE_CTE_NODE") == 0) {
		return QueryNodeType::RECURSIVE_CTE_NODE;
	} else {
		throw NotImplementedException("StringToEnum not implemented for string value");
	}
}

template <>
const char *EnumToString(QueryNodeType value) {
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
		throw NotImplementedException("EnumToString not implemented for enum value");
	}
}

//-----------------------------------------------
// SetOperationType
//-----------------------------------------------
template <>
SetOperationType StringToEnum(const char *value) {
	if (strcmp(value, "NONE") == 0) {
		return SetOperationType::NONE;
	} else if (strcmp(value, "UNION") == 0) {
		return SetOperationType::UNION;
	} else if (strcmp(value, "EXCEPT") == 0) {
		return SetOperationType::EXCEPT;
	} else if (strcmp(value, "INTERSECT") == 0) {
		return SetOperationType::INTERSECT;
	} else if (strcmp(value, "UNION_BY_NAME") == 0) {
		return SetOperationType::UNION_BY_NAME;
	} else {
		throw NotImplementedException("StringToEnum not implemented for enum value");
	}
}

template <>
const char *EnumToString(SetOperationType value) {
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
		throw NotImplementedException("EnumToString not implemented for enum value");
	}
}

//-----------------------------------------------
// WindowBoundary
//-----------------------------------------------
template <>
WindowBoundary StringToEnum(const char *value) {
	if (strcmp(value, "INVALID") == 0) {
		return WindowBoundary::INVALID;
	} else if (strcmp(value, "UNBOUNDED_PRECEDING") == 0) {
		return WindowBoundary::UNBOUNDED_PRECEDING;
	} else if (strcmp(value, "UNBOUNDED_FOLLOWING") == 0) {
		return WindowBoundary::UNBOUNDED_FOLLOWING;
	} else if (strcmp(value, "CURRENT_ROW_RANGE") == 0) {
		return WindowBoundary::CURRENT_ROW_RANGE;
	} else if (strcmp(value, "CURRENT_ROW_ROWS") == 0) {
		return WindowBoundary::CURRENT_ROW_ROWS;
	} else if (strcmp(value, "EXPR_PRECEDING_ROWS") == 0) {
		return WindowBoundary::EXPR_PRECEDING_ROWS;
	} else if (strcmp(value, "EXPR_FOLLOWING_ROWS") == 0) {
		return WindowBoundary::EXPR_FOLLOWING_ROWS;
	} else if (strcmp(value, "EXPR_PRECEDING_RANGE") == 0) {
		return WindowBoundary::EXPR_PRECEDING_RANGE;
	} else if (strcmp(value, "EXPR_FOLLOWING_RANGE") == 0) {
		return WindowBoundary::EXPR_FOLLOWING_RANGE;
	} else {
		throw NotImplementedException("FromString not implemented for enum value");
	}
}

template <>
const char *EnumToString(WindowBoundary value) {
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
SubqueryType StringToEnum(const char *value) {
	if (strcmp(value, "INVALID") == 0) {
		return SubqueryType::INVALID;
	} else if (strcmp(value, "SCALAR") == 0) {
		return SubqueryType::SCALAR;
	} else if (strcmp(value, "EXISTS") == 0) {
		return SubqueryType::EXISTS;
	} else if (strcmp(value, "NOT_EXISTS") == 0) {
		return SubqueryType::NOT_EXISTS;
	} else if (strcmp(value, "ANY") == 0) {
		return SubqueryType::ANY;
	} else {
		throw NotImplementedException("StringToEnum not implemented for enum value");
	}
}

template <>
const char *EnumToString(SubqueryType value) {
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
		throw NotImplementedException("EnumToString not implemented for enum value");
	}
}

//-----------------------------------------------
// ExpressionType
//-----------------------------------------------
template <>
ExpressionType StringToEnum(const char *value) {
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

template <>
const char *EnumToString(ExpressionType value) {
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
ExpressionClass StringToEnum(const char *value) {
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

template <>
const char *EnumToString(ExpressionClass value) {
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
SampleMethod StringToEnum(const char *value) {
	if (strcmp(value, "System") == 0) {
		return SampleMethod::SYSTEM_SAMPLE;
	} else if (strcmp(value, "Bernoulli") == 0) {
		return SampleMethod::BERNOULLI_SAMPLE;
	} else if (strcmp(value, "Reservoir") == 0) {
		return SampleMethod::RESERVOIR_SAMPLE;
	} else {
		throw NotImplementedException("Unrecognized sample method type \"%s\"", value);
	}
}

template <>
const char *EnumToString(SampleMethod value) {
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
LogicalTypeId StringToEnum(const char *value) {
	if (strcmp(value, "BOOLEAN") == 0) {
		return LogicalTypeId::BOOLEAN;
	} else if (strcmp(value, "TINYINT") == 0) {
		return LogicalTypeId::TINYINT;
	} else if (strcmp(value, "SMALLINT") == 0) {
		return LogicalTypeId::SMALLINT;
	} else if (strcmp(value, "INTEGER") == 0) {
		return LogicalTypeId::INTEGER;
	} else if (strcmp(value, "BIGINT") == 0) {
		return LogicalTypeId::BIGINT;
	} else if (strcmp(value, "HUGEINT") == 0) {
		return LogicalTypeId::HUGEINT;
	} else if (strcmp(value, "UUID") == 0) {
		return LogicalTypeId::UUID;
	} else if (strcmp(value, "UTINYINT") == 0) {
		return LogicalTypeId::UTINYINT;
	} else if (strcmp(value, "USMALLINT") == 0) {
		return LogicalTypeId::USMALLINT;
	} else if (strcmp(value, "UINTEGER") == 0) {
		return LogicalTypeId::UINTEGER;
	} else if (strcmp(value, "UBIGINT") == 0) {
		return LogicalTypeId::UBIGINT;
	} else if (strcmp(value, "DATE") == 0) {
		return LogicalTypeId::DATE;
	} else if (strcmp(value, "TIME") == 0) {
		return LogicalTypeId::TIME;
	} else if (strcmp(value, "TIMESTAMP") == 0) {
		return LogicalTypeId::TIMESTAMP;
	} else if (strcmp(value, "TIMESTAMP_MS") == 0) {
		return LogicalTypeId::TIMESTAMP_MS;
	} else if (strcmp(value, "TIMESTAMP_NS") == 0) {
		return LogicalTypeId::TIMESTAMP_NS;
	} else if (strcmp(value, "TIMESTAMP_S") == 0) {
		return LogicalTypeId::TIMESTAMP_SEC;
	} else if (strcmp(value, "TIMESTAMP WITH TIME ZONE") == 0) {
		return LogicalTypeId::TIMESTAMP_TZ;
	} else if (strcmp(value, "TIME WITH TIME ZONE") == 0) {
		return LogicalTypeId::TIME_TZ;
	} else if (strcmp(value, "FLOAT") == 0) {
		return LogicalTypeId::FLOAT;
	} else if (strcmp(value, "DOUBLE") == 0) {
		return LogicalTypeId::DOUBLE;
	} else if (strcmp(value, "DECIMAL") == 0) {
		return LogicalTypeId::DECIMAL;
	} else if (strcmp(value, "VARCHAR") == 0) {
		return LogicalTypeId::VARCHAR;
	} else if (strcmp(value, "BLOB") == 0) {
		return LogicalTypeId::BLOB;
	} else if (strcmp(value, "CHAR") == 0) {
		return LogicalTypeId::CHAR;
	} else if (strcmp(value, "INTERVAL") == 0) {
		return LogicalTypeId::INTERVAL;
	} else if (strcmp(value, "NULL") == 0) {
		return LogicalTypeId::SQLNULL;
	} else if (strcmp(value, "ANY") == 0) {
		return LogicalTypeId::ANY;
	} else if (strcmp(value, "VALIDITY") == 0) {
		return LogicalTypeId::VALIDITY;
	} else if (strcmp(value, "STRUCT") == 0) {
		return LogicalTypeId::STRUCT;
	} else if (strcmp(value, "LIST") == 0) {
		return LogicalTypeId::LIST;
	} else if (strcmp(value, "MAP") == 0) {
		return LogicalTypeId::MAP;
	} else if (strcmp(value, "POINTER") == 0) {
		return LogicalTypeId::POINTER;
	} else if (strcmp(value, "TABLE") == 0) {
		return LogicalTypeId::TABLE;
	} else if (strcmp(value, "LAMBDA") == 0) {
		return LogicalTypeId::LAMBDA;
	} else if (strcmp(value, "INVALID") == 0) {
		return LogicalTypeId::INVALID;
	} else if (strcmp(value, "UNION") == 0) {
		return LogicalTypeId::UNION;
	} else if (strcmp(value, "UNKNOWN") == 0) {
		return LogicalTypeId::UNKNOWN;
	} else if (strcmp(value, "ENUM") == 0) {
		return LogicalTypeId::ENUM;
	} else if (strcmp(value, "AGGREGATE_STATE") == 0) {
		return LogicalTypeId::AGGREGATE_STATE;
	} else if (strcmp(value, "USER") == 0) {
		return LogicalTypeId::USER;
	} else if (strcmp(value, "BIT") == 0) {
		return LogicalTypeId::BIT;
	} else {
		throw NotImplementedException("Unrecognized type LogicalTypeId in EnumSerializer::StringToEnum");
	}
}

template <>
const char *EnumToString(LogicalTypeId value) {
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

} // namespace EnumSerializer

} // namespace duckdb
