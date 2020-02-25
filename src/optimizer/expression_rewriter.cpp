#include "duckdb/optimizer/expression_rewriter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionRewriter::ApplyRules(LogicalOperator &op, const vector<Rule *> &rules,
                                                      unique_ptr<Expression> expr, bool &changes_made) {
	for (auto &rule : rules) {
		vector<Expression *> bindings;
		if (rule->root->Match(expr.get(), bindings)) {
			// the rule matches! try to apply it
			bool rule_made_change = false;
			auto result = rule->Apply(op, bindings, rule_made_change);
			if (result) {
				changes_made = true;
				// the base node changed: the rule applied changes
				// rerun on the new node
				return ExpressionRewriter::ApplyRules(op, rules, move(result), changes_made);
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
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		return ExpressionRewriter::ApplyRules(op, rules, move(child), changes_made);
	});
	return expr;
}

void ExpressionRewriter::Apply(LogicalOperator &root) {
	// first apply the rules to child operators of this node (if any)
	for (auto &child : root.children) {
		Apply(*child);
	}
	// apply the rules to this node
	if (root.expressions.size() == 0) {
		// no expressions to apply rules on: return
		return;
	}
	vector<Rule *> to_apply_rules;
	for (auto &rule : rules) {
		if (rule->logical_root && !rule->logical_root->Match(root.type)) {
			// this rule does not apply to this type of LogicalOperator
			continue;
		}
		to_apply_rules.push_back(rule.get());
	}
	if (to_apply_rules.size() == 0) {
		// no rules to apply on this node
		return;
	}
	for (idx_t i = 0; i < root.expressions.size(); i++) {
		bool changes_made;
		do {
			changes_made = false;
			root.expressions[i] =
			    ExpressionRewriter::ApplyRules(root, to_apply_rules, move(root.expressions[i]), changes_made);
		} while (changes_made);
	}

	// if it is a LogicalFilter, we split up filter conjunctions again
	if (root.type == LogicalOperatorType::FILTER) {
		auto &filter = (LogicalFilter &)root;
		filter.SplitPredicates();
	}
}
