#include "duckdb/optimizer/expression_heuristics.hpp"
#include <iostream>

#include "duckdb/planner/expression/list.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> ExpressionHeuristics::Rewrite(unique_ptr<LogicalOperator> op) {
    VisitOperator(*op);
    return op;
}

void ExpressionHeuristics::VisitOperator(LogicalOperator &op) {
    if (op.type == LogicalOperatorType::FILTER) {
        //reorder all filter expressions
        ReorderExpressions(op.expressions);
    }

    //traverse recursively through the operator tree
    VisitOperatorChildren(op);
}

void ExpressionHeuristics::ReorderExpressions(vector<unique_ptr<Expression>> &expressions) {
    if (expressions.size() != 1) {
        //iterate expressions, get score for each one and order by that score
		for (index_t i = 0; i < expressions.size(); i++) {
			Cost(*expressions[i]); //TODO: get scores, reorder
		}
    }
}

index_t ExpressionHeuristics::Cost(Expression &expr) {

	switch (expr.expression_class) {
		case ExpressionClass::BOUND_COMPARISON: {
			auto &comp_expr = (BoundComparisonExpression &)expr;

			//COMPARE_EQUAL, COMPARE_NOTEQUAL, COMPARE_GREATERTHAN, COMPARE_GREATERTHANOREQUALTO, COMPARE_LESSTHAN, COMPARE_LESSTHANOREQUALTO
			return Cost(*comp_expr.left) + 5 + Cost(*comp_expr.right);

			//TODO: did I forget anything?
			//TODO: what about COMPARE_BOUNDARY_START?
		}
		case ExpressionClass::BOUND_OPERATOR: {
			auto &op_expr = (BoundOperatorExpression &)expr;

			index_t sum = 0;
			for (auto &child : op_expr.children) {
				sum += Cost(*child);
			}

			//OPERATOR_IS_NULL, OPERATOR_IS_NOT_NULL
			if (expr.type == ExpressionType::OPERATOR_IS_NULL || expr.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
				return sum + 5;
			}

			//COMPARE_IN, COMPARE_NOT_IN
			if (expr.type == ExpressionType::COMPARE_IN || expr.type == ExpressionType::COMPARE_NOT_IN) {
				return sum + op_expr.children.size() * 100;
			}

			break;
		}
		case ExpressionClass::BOUND_FUNCTION: {
			auto &func_expr = (BoundFunctionExpression &)expr;

			index_t sum = 0;
			for (auto &child : func_expr.children) {
				sum += Cost(*child);
			}

			auto func_name = func_expr.function.name;

			//LIKE and NOT LIKE
			if (func_name == "~~" || func_name == "!~~" || func_name == "regexp_matches") {
				return sum + 200;
			}

			break;
		}
		default: {
			break;
		}
	}

	return 0;

    /*
    // explicitly cast left as right (right is integer in ValueType enum)
	OPERATOR_CAST = 12,
	// logical not operator
	OPERATOR_NOT = 13,

	// IS DISTINCT FROM operator
	COMPARE_DISTINCT_FROM = 37,
	// compare final boundary

	COMPARE_BETWEEN = 38,
	COMPARE_NOT_BETWEEN = 39,
	COMPARE_BOUNDARY_END = COMPARE_NOT_BETWEEN,

	CONJUNCTION_AND = 50,
	CONJUNCTION_OR = 51,

	VALUE_CONSTANT = 75,
	VALUE_PARAMETER = 76,
	VALUE_TUPLE = 77,
	VALUE_TUPLE_ADDRESS = 78,
	VALUE_NULL = 79,
	VALUE_VECTOR = 80,
	VALUE_SCALAR = 81,
	VALUE_DEFAULT = 82,

	AGGREGATE = 100,
	BOUND_AGGREGATE = 101,

	WINDOW_AGGREGATE = 110,

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

	FUNCTION = 140,
	BOUND_FUNCTION = 141,

	CASE_EXPR = 150,
	OPERATOR_NULLIF = 151,
	OPERATOR_COALESCE = 152,

	SUBQUERY = 175,

	STAR = 200,
	PLACEHOLDER = 201,
	COLUMN_REF = 202,
	FUNCTION_REF = 203,
	TABLE_REF = 204,

	CAST = 225,
	COMMON_SUBEXPRESSION = 226,
	BOUND_REF = 227,
	BOUND_COLUMN_REF = 228
	*/
}