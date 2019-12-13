#include "duckdb/optimizer/expression_heuristics.hpp"
#include <iostream>

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
        /* 
        *op->expressions[0]
        p ((BaseExpression *)(op->expressions[1])).type 
        $13 = duckdb::ExpressionType::COMPARE_EQUAL

        p *((BoundColumnRefExpression *)(((BoundComparisonExpression *)(op->expressions[1])).left)) 
        $26 = {<duckdb::Expression> = {<duckdb::BaseExpression> = {
            _vptr.BaseExpression = 0x7ffff7d8ad40 <vtable for duckdb::BoundColumnRefExpression+16>, 
            type = -28, expression_class = duckdb::ExpressionClass::BOUND_COLUMN_REF, alias = "b"}, 
            return_type = duckdb::TypeId::INTEGER}, binding = {table_index = 4, column_index = 1}, 
        depth = 0}1
        */
    }

    //traverse recursively through the operator tree
    VisitOperatorChildren(op);
}

void ExpressionHeuristics::ReorderExpressions(vector<unique_ptr<Expression>> &expressions) {
    if (expressions.size() != 1) {
        //logic to reorder expressions, traverse tree and calculate scores
        //TODO: iterate over expressions, get score for each one and order by that score
        //ReturnCost(expressions[0]);
    }
}

index_t ExpressionHeuristics::ReturnCost(unique_ptr<Expression> &expr) {

    //TODO: add recursive calls or return values for each expression type
    switch(expr->type) {
        case ExpressionType::COMPARE_EQUAL:
            return 20; //actually return ReturnCost(left) + 5 + ReturnCost(right);
        case ExpressionType::OPERATOR_IS_NULL:
            return 42;
        default:
            return 1000;
    }

    /*
    // explicitly cast left as right (right is integer in ValueType enum)
	OPERATOR_CAST = 12,
	// logical not operator
	OPERATOR_NOT = 13,
	// is null operator
	OPERATOR_IS_NULL = 14,
	// is not null operator
	OPERATOR_IS_NOT_NULL = 15,

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
	// IN operator [left IN (right1, right2, ...)]
	COMPARE_IN = 35,
	// NOT IN operator [left NOT IN (right1, right2, ...)]
	COMPARE_NOT_IN = 36,
	// IS DISTINCT FROM operator
	COMPARE_DISTINCT_FROM = 37,
	// compare final boundary

	COMPARE_BETWEEN = 38,
	COMPARE_NOT_BETWEEN = 39,
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
	AGGREGATE = 100,
	BOUND_AGGREGATE = 101,

	// -----------------------------
	// Window Functions
	// -----------------------------
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

	// -----------------------------
	// Functions
	// -----------------------------
	FUNCTION = 140,
	BOUND_FUNCTION = 141,

	// -----------------------------
	// Operators
	// -----------------------------
	CASE_EXPR = 150,
	OPERATOR_NULLIF = 151,
	OPERATOR_COALESCE = 152,

	// -----------------------------
	// Subquery IN/EXISTS
	// -----------------------------
	SUBQUERY = 175,

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
	BOUND_COLUMN_REF = 228 */
}