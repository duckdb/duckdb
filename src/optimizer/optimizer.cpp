#include "optimizer/optimizer.hpp"

#include "optimizer/expression_rules/list.hpp"
#include "optimizer/join_order_optimizer.hpp"
#include "optimizer/logical_rules/list.hpp"
#include "optimizer/subquery_rewriter.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

Optimizer::Optimizer(BindContext &context) : context(context), rewriter(context) {
	rewriter.rules.push_back(make_unique<ConstantCastRule>());
	rewriter.rules.push_back(make_unique<ConstantFoldingRule>());
	rewriter.rules.push_back(make_unique<DistributivityRule>());
	rewriter.rules.push_back(make_unique<SplitFilterConjunctionRule>());
	// rewriter.rules.push_back(make_unique<InClauseRewriteRule>());
	// rewriter.rules.push_back(make_unique<ExistsRewriteRule>());
	// rewriter.rules.push_back(make_unique<SubqueryRewritingRule>());
	rewriter.rules.push_back(make_unique<RemoveObsoleteFilterRule>());

#ifdef DEBUG
	for (auto &rule : rewriter.rules) {
		// root not defined in rule
		assert(rule->root);
	}
#endif
}

unique_ptr<LogicalOperator> Optimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	// first we perform expression rewrites
	// this does not change the logical plan structure yet, but only simplifies expression trees
	auto new_plan = rewriter.ApplyRules(move(plan));
	// then we perform the join ordering optimization
	// this also rewrites cross products + filters into joins and performs filter pushdowns
	JoinOrderOptimizer optimizer;
	auto join_order = optimizer.Optimize(move(new_plan));
	// finally we rewrite subqueries
	SubqueryRewriter subquery_rewriter(context);
	return subquery_rewriter.Rewrite(move(join_order));
}
