#include "optimizer/expression_rewriter.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionRewriter::ApplyRules(const vector<Rule*> &rules, unique_ptr<Expression> expr) {
	for(auto &rule : rules) {
		vector<Expression*> bindings;
		if (rule->root->Match(expr.get(), bindings)) {
			// the rule matches! try to apply it
			bool changes_made = false;
			auto result = rule->Apply(bindings, changes_made);
			if (result) {
				// the base node changed: the rule applied changes
				// rerun on the new node
				return ExpressionRewriter::ApplyRules(rules, move(result));
			} else if (changes_made) {
				// the base node didn't change, but changes were made, rerun
				return ExpressionRewriter::ApplyRules(rules, move(expr));
			}
			// else nothing changed, continue to the next rule
			continue;
		}
	}
	// no changes could be made to this node
	// recursively run on the children of this node
	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		return ExpressionRewriter::ApplyRules(rules, move(child));
	});
	return expr;
}

void ExpressionRewriter::Apply(LogicalOperator& root) {
	// first apply the rules to child operators of this node (if any)
	for(auto &child : root.children) {
		Apply(*child);
	}
	// apply the rules to this node
	if (root.expressions.size() == 0) {
		// no expressions to apply rules on: return
		return;
	}
	vector<Rule*> to_apply_rules;
	for(auto &rule : rules) {
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
	for(size_t i = 0; i < root.expressions.size(); i++) {
		root.expressions[i] = ExpressionRewriter::ApplyRules(to_apply_rules, move(root.expressions[i]));
	}
}
