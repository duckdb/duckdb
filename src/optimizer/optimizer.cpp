
#include "optimizer/optimizer.hpp"
#include "optimizer/expression_rules/rule_list.hpp"
#include "optimizer/logical_rules/rule_list.hpp"

#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

Optimizer::Optimizer(BindContext &context) : rewriter(context) {
	rewriter.rules.push_back(make_unique<ConstantCastRule>());
	rewriter.rules.push_back(make_unique<ConstantFoldingRule>());
	rewriter.rules.push_back(make_unique<DistributivityRule>());
	rewriter.rules.push_back(make_unique<SplitFilterConjunctionRule>());
	rewriter.rules.push_back(make_unique<InClauseRewriteRule>());
	rewriter.rules.push_back(make_unique<ExistsRewriteRule>());
	rewriter.rules.push_back(make_unique<SubqueryRewritingRule>());
	rewriter.rules.push_back(make_unique<CrossProductRewrite>());
	rewriter.rules.push_back(make_unique<SelectionPushdownRule>());

#ifdef DEBUG
	for (auto &rule : rewriter.rules) {
		// root not defined in rule
		assert(rule->root);
	}
#endif
}

unique_ptr<LogicalOperator>
Optimizer::Optimize(unique_ptr<LogicalOperator> plan) {
    // then we optimize the logical tree
    return rewriter.ApplyRules(move(plan));
}
