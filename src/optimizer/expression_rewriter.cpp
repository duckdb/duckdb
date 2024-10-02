#include "duckdb/optimizer/expression_rewriter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

unique_ptr<Expression> ExpressionRewriter::ApplyRules(LogicalOperator &op, const vector<reference<Rule>> &rules,
                                                      unique_ptr<Expression> expr, bool &changes_made, bool is_root) {
	for (auto &rule : rules) {
		vector<reference<Expression>> bindings;
		if (rule.get().root->Match(*expr, bindings)) {
			// the rule matches! try to apply it
			bool rule_made_change = false;
			auto result = rule.get().Apply(op, bindings, rule_made_change, is_root);
			if (result) {
				changes_made = true;
				// the base node changed: the rule applied changes
				// rerun on the new node
				return ExpressionRewriter::ApplyRules(op, rules, std::move(result), changes_made);
			} else if (rule_made_change) {
				changes_made = true;
				// the base node didn't change, but changes were made, rerun
				return expr;
			}
			// else nothing changed, continue to the next rule
			continue;
		}
	}
	// no changes could be made to this node
	// recursively run on the children of this node
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		child = ExpressionRewriter::ApplyRules(op, rules, std::move(child), changes_made);
	});
	return expr;
}

unique_ptr<Expression> ExpressionRewriter::ConstantOrNull(unique_ptr<Expression> child, Value value) {
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(value));
	children.push_back(std::move(child));
	return ConstantOrNull(std::move(children), std::move(value));
}

unique_ptr<Expression> ExpressionRewriter::ConstantOrNull(vector<unique_ptr<Expression>> children, Value value) {
	auto type = value.type();
	children.insert(children.begin(), make_uniq<BoundConstantExpression>(value));
	return make_uniq<BoundFunctionExpression>(type, ConstantOrNull::GetFunction(type), std::move(children),
	                                          ConstantOrNull::Bind(std::move(value)));
}

void ExpressionRewriter::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	this->op = &op;

	to_apply_rules.clear();
	for (auto &rule : rules) {
		to_apply_rules.push_back(*rule);
	}

	VisitOperatorExpressions(op);

	// if it is a LogicalFilter, we split up filter conjunctions again
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		filter.SplitPredicates();
	}
}

void ExpressionRewriter::VisitExpression(unique_ptr<Expression> *expression) {
	bool changes_made;
	do {
		changes_made = false;
		*expression = ExpressionRewriter::ApplyRules(*op, to_apply_rules, std::move(*expression), changes_made, true);
	} while (changes_made);
}

ClientContext &Rule::GetContext() const {
	return rewriter.context;
}

} // namespace duckdb
