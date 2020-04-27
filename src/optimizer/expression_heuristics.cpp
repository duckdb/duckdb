#include "duckdb/optimizer/expression_heuristics.hpp"
#include "duckdb/planner/expression/list.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> ExpressionHeuristics::Rewrite(unique_ptr<LogicalOperator> op) {
	VisitOperator(*op);
	return op;
}

void ExpressionHeuristics::VisitOperator(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::FILTER) {
		// reorder all filter expressions
		if (op.expressions.size() > 1) {
			ReorderExpressions(op.expressions);
		}
	}

	// traverse recursively through the operator tree
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
}

unique_ptr<Expression> ExpressionHeuristics::VisitReplace(BoundConjunctionExpression &expr,
                                                          unique_ptr<Expression> *expr_ptr) {
	ReorderExpressions(expr.children);
	return nullptr;
}

void ExpressionHeuristics::ReorderExpressions(vector<unique_ptr<Expression>> &expressions) {

	struct ExpressionCosts {
		unique_ptr<Expression> expr;
		idx_t cost;

		bool operator==(const ExpressionCosts &p) const {
			return cost == p.cost;
		}
		bool operator<(const ExpressionCosts &p) const {
			return cost < p.cost;
		}
	};

	vector<ExpressionCosts> expression_costs;
	// iterate expressions, get cost for each one
	for (idx_t i = 0; i < expressions.size(); i++) {
		idx_t cost = Cost(*expressions[i]);
		expression_costs.push_back({move(expressions[i]), cost});
	}

	// sort by cost and put back in place
	sort(expression_costs.begin(), expression_costs.end());
	for (idx_t i = 0; i < expression_costs.size(); i++) {
		expressions[i] = move(expression_costs[i].expr);
	}
}

idx_t ExpressionHeuristics::ExpressionCost(BoundBetweenExpression &expr) {
	return Cost(*expr.input) + Cost(*expr.lower) + Cost(*expr.upper) + 10;
}

idx_t ExpressionHeuristics::ExpressionCost(BoundCaseExpression &expr) {
	// CASE WHEN check THEN result_if_true ELSE result_if_false END
	return Cost(*expr.check) + Cost(*expr.result_if_true) + Cost(*expr.result_if_false) + 5;
}

idx_t ExpressionHeuristics::ExpressionCost(BoundCastExpression &expr) {
	// OPERATOR_CAST
	// determine cast cost by comparing cast_expr.source_type and cast_expr_target_type
	idx_t cast_cost = 0;
	if (expr.target_type != expr.source_type) {
		// if cast from or to varchar
		// TODO: we might want to add more cases
		if (expr.target_type == SQLType::VARCHAR || expr.source_type == SQLType::VARCHAR) {
			cast_cost = 200;
		} else {
			cast_cost = 5;
		}
	}
	return Cost(*expr.child) + cast_cost;
}

idx_t ExpressionHeuristics::ExpressionCost(BoundComparisonExpression &expr) {
	// COMPARE_EQUAL, COMPARE_NOTEQUAL, COMPARE_GREATERTHAN, COMPARE_GREATERTHANOREQUALTO, COMPARE_LESSTHAN,
	// COMPARE_LESSTHANOREQUALTO
	return Cost(*expr.left) + 5 + Cost(*expr.right);
}

idx_t ExpressionHeuristics::ExpressionCost(BoundConjunctionExpression &expr) {
	// CONJUNCTION_AND, CONJUNCTION_OR
	idx_t cost = 5;
	for (auto &child : expr.children) {
		cost += Cost(*child);
	}
	return cost;
}

idx_t ExpressionHeuristics::ExpressionCost(BoundFunctionExpression &expr) {
	idx_t cost_children = 0;
	for (auto &child : expr.children) {
		cost_children += Cost(*child);
	}

	auto cost_function = function_costs.find(expr.function.name);
	if (cost_function != function_costs.end()) {
		return cost_children + cost_function->second;
	} else {
		return cost_children + 1000;
	}
}

idx_t ExpressionHeuristics::ExpressionCost(BoundOperatorExpression &expr, ExpressionType &expr_type) {
	idx_t sum = 0;
	for (auto &child : expr.children) {
		sum += Cost(*child);
	}

	// OPERATOR_IS_NULL, OPERATOR_IS_NOT_NULL
	if (expr_type == ExpressionType::OPERATOR_IS_NULL || expr_type == ExpressionType::OPERATOR_IS_NOT_NULL) {
		return sum + 5;
	} else if (expr_type == ExpressionType::COMPARE_IN || expr_type == ExpressionType::COMPARE_NOT_IN) {
		// COMPARE_IN, COMPARE_NOT_IN
		return sum + (expr.children.size() - 1) * 100;
	} else if (expr_type == ExpressionType::OPERATOR_NOT) {
		// OPERATOR_NOT
		return sum + 10; // TODO: evaluate via measured runtimes
	} else {
		return sum + 1000;
	}
}

idx_t ExpressionHeuristics::ExpressionCost(TypeId &return_type, idx_t multiplier) {
	// TODO: ajust values according to benchmark results
	switch (return_type) {
	case TypeId::VARCHAR:
		return 5 * multiplier;
	case TypeId::FLOAT:
		return 2 * multiplier;
	case TypeId::DOUBLE:
		return 2 * multiplier;
	default:
		return 1 * multiplier;
	}
}

idx_t ExpressionHeuristics::Cost(Expression &expr) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = (BoundCaseExpression &)expr;
		return ExpressionCost(case_expr);
	}
	case ExpressionClass::BOUND_BETWEEN: {
		auto &between_expr = (BoundBetweenExpression &)expr;
		return ExpressionCost(between_expr);
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast_expr = (BoundCastExpression &)expr;
		return ExpressionCost(cast_expr);
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp_expr = (BoundComparisonExpression &)expr;
		return ExpressionCost(comp_expr);
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj_expr = (BoundConjunctionExpression &)expr;
		return ExpressionCost(conj_expr);
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func_expr = (BoundFunctionExpression &)expr;
		return ExpressionCost(func_expr);
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op_expr = (BoundOperatorExpression &)expr;
		return ExpressionCost(op_expr, expr.type);
	}
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &col_expr = (BoundColumnRefExpression &)expr;
		return ExpressionCost(col_expr.return_type, 8);
	}
	case ExpressionClass::BOUND_CONSTANT: {
		auto &const_expr = (BoundConstantExpression &)expr;
		return ExpressionCost(const_expr.return_type, 1);
	}
	case ExpressionClass::BOUND_PARAMETER: {
		auto &const_expr = (BoundConstantExpression &)expr;
		return ExpressionCost(const_expr.return_type, 1);
	}
	case ExpressionClass::BOUND_REF: {
		auto &col_expr = (BoundColumnRefExpression &)expr;
		return ExpressionCost(col_expr.return_type, 8);
	}
	default: { break; }
	}

	// return a very high value if nothing matches
	return 1000;
}
