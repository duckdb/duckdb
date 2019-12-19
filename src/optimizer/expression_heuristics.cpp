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

	struct expr {
		index_t idx;
		index_t cost;

		bool operator==(const expr &p) const {
		return idx == p.idx && cost == p.cost;
		}
		bool operator<(const expr &p) const {
		return cost < p.cost || (cost == p.cost && idx < p.idx);
		}
	};

	vector<expr> exprCosts;

    if (expressions.size() > 1) {
        //iterate expressions, get cost for each one and order by that cost
		for (index_t i = 0; i < expressions.size(); i++) {
			exprCosts.push_back({i, Cost(*expressions[i])});
		}

		sort(exprCosts.begin(), exprCosts.end());

		assert(exprCosts.size() == expressions.size());

		//for all elements to put in place
		for(index_t i = 0; i < expressions.size() - 1; ++i) {
			//while the element i is not yet in place
			while(i != exprCosts[i].idx) {
				//swap it with the element at its final place
				index_t alt = exprCosts[i].idx;
				swap(expressions[i], expressions[alt]);
				swap(exprCosts[i], exprCosts[alt]);
			}
		}
    }
}

index_t ExpressionHeuristics::Cost(Expression &expr) {
	switch (expr.expression_class) {
		case ExpressionClass::BOUND_CASE: {
			auto &case_expr = (BoundCaseExpression &)expr;

			//CASE WHEN check THEN result_if_true ELSE result_if_false END
			return Cost(*case_expr.check) + Cost(*case_expr.result_if_true) + Cost(*case_expr.result_if_false) + 5;
		}
		case ExpressionClass::BOUND_CAST: {
			//OPERATOR_CAST
			auto &cast_expr = (BoundCastExpression &)expr;

			//determine cast cost by comparing cast_expr.source_type and cast_expr_target_type
			index_t cast_cost = 0;
			if (cast_expr.target_type != cast_expr.source_type) {
				//if cast from or to varchar
				//TODO: we might want to add more cases
				if (cast_expr.target_type == SQLType::VARCHAR || cast_expr.source_type == SQLType::VARCHAR) {
					cast_cost = 200;
				} else {
					cast_cost = 5;
				}
			}

			return Cost(*cast_expr.child) + cast_cost;
		}
		case ExpressionClass::BOUND_COMPARISON: {
			auto &comp_expr = (BoundComparisonExpression &)expr;

			//COMPARE_EQUAL, COMPARE_NOTEQUAL, COMPARE_GREATERTHAN, COMPARE_GREATERTHANOREQUALTO, COMPARE_LESSTHAN, COMPARE_LESSTHANOREQUALTO
			return Cost(*comp_expr.left) + 5 + Cost(*comp_expr.right);
		}
		case ExpressionClass::BOUND_CONJUNCTION: {
			auto &conj_expr = (BoundConjunctionExpression &)expr;

			//CONJUNCTION_AND, CONJUNCTION_OR
			return Cost(*conj_expr.left) + 5 + Cost(*conj_expr.right);
		}
		case ExpressionClass::BOUND_FUNCTION: {
			auto &func_expr = (BoundFunctionExpression &)expr;

			index_t cost_children = 0;
			for (auto &child : func_expr.children) {
				cost_children += Cost(*child);
			}

			auto func_name = func_expr.function.name;

			//ADD(+), SUB(-), BITWISE AND(&), BITWISE OR(#), RIGHTSHIFT(>>), LEFTSHIFT(<<), abs()
			if (func_name == "+" || func_name == "-" || func_name == "&" || func_name == "#" || func_name == ">>" || func_name == "<<" || func_name == "abs") {
				return cost_children + 5;
			}

			//TIMES(*), MOD(%)
			if (func_name == "*" || func_name == "%") {
				return cost_children + 10;
			}

			//DIV(/)
			if (func_name == "/") {
				return cost_children + 15;
			}

			//extract()/date_part(), year()
			if (func_name == "date_part" || func_name == "year") {
				return cost_children + 20; //TODO: validate against benchmark results
			}

			//round()
			if (func_name == "round") {
				return cost_children + 100;
			}

			//(NOT) LIKE, SIMILAR TO, CONCAT(||)
			if (func_name == "~~" || func_name == "!~~" || func_name == "regexp_matches" || func_name == "||") {
				return cost_children + 200;
			}

			break;
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
				return sum + (op_expr.children.size() - 1) * 100;
			}

			//OPERATOR_NOT
			if (expr.type == ExpressionType::OPERATOR_NOT) {
				return sum + 10; //TODO: evaluate via measured runtimes
			}

			break;
		}
		case ExpressionClass::BOUND_COLUMN_REF: {
			auto &col_expr = (BoundColumnRefExpression &)expr;

			//TODO: ajust values according to benchmark results
			switch (col_expr.return_type) {
				case TypeId::VARCHAR:
					return 40;
				case TypeId::FLOAT:
					return 10;
				case TypeId::DOUBLE:
					return 10;
				default:
					return 5;
			}
		}
		case ExpressionClass::BOUND_CONSTANT: {
			auto &const_expr = (BoundConstantExpression &)expr;

			//VALUE_CONSTANT
			//TODO: ajust values according to benchmark results
			switch (const_expr.return_type) {
				case TypeId::VARCHAR:
					return 5;
				case TypeId::FLOAT:
					return 2;
				case TypeId::DOUBLE:
					return 2;
				default:
					return 1;
			}
		}
		case ExpressionClass::BOUND_PARAMETER: {
			auto &const_expr = (BoundConstantExpression &)expr;

			//TODO: VALUE_PARAMETER?
			//TODO: ajust values according to benchmark results
			switch (const_expr.return_type) {
				case TypeId::VARCHAR:
					return 5;
				case TypeId::FLOAT:
					return 2;
				case TypeId::DOUBLE:
					return 2;
				default:
					return 1;
			}
		}
		case ExpressionClass::BOUND_REF: {
			auto &col_expr = (BoundColumnRefExpression &)expr;

			//TODO: ajust values according to benchmark results
			switch (col_expr.return_type) {
				case TypeId::VARCHAR:
					return 40;
				case TypeId::FLOAT:
					return 10;
				case TypeId::DOUBLE:
					return 10;
				default:
					return 5;
			}
		}
		default: {
			break;
		}
	}

	//return a very high value if nothing matches
	return 1000;
}