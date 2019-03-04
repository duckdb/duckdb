#include "optimizer/optimizer.hpp"

#include "optimizer/cse_optimizer.hpp"
#include "optimizer/filter_pushdown.hpp"
#include "optimizer/join_order_optimizer.hpp"
#include "optimizer/obsolete_filter_rewriter.hpp"
#include "optimizer/rule/list.hpp"
#include "parser/expression/common_subexpression.hpp"

#include "planner/binder.hpp"


using namespace duckdb;
using namespace std;

Optimizer::Optimizer(Binder &binder, ClientContext &client_context) : binder(binder), rewriter(client_context) {
	rewriter.rules.push_back(make_unique<ConstantFoldingRule>(rewriter));
	rewriter.rules.push_back(make_unique<DistributivityRule>(rewriter));
	rewriter.rules.push_back(make_unique<ArithmeticSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<CaseSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<ConjunctionSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<ComparisonSimplificationRule>(rewriter));

#ifdef DEBUG
	for (auto &rule : rewriter.rules) {
		// root not defined in rule
		assert(rule->root);
	}
#endif
}

unique_ptr<LogicalOperator> Optimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	// first we perform expression rewrites using the ExpressionRewriter
	// this does not change the logical plan structure, but only simplifies the expression trees
	rewriter.Apply(*plan);
	// perform filter pushdown
	FilterPushdown filter_pushdown(*this);
	plan = filter_pushdown.Rewrite(move(plan));
	// perform obsolete filter removal
	ObsoleteFilterRewriter obsolete_filter;
	plan = obsolete_filter.Rewrite(move(plan));
	// then we perform the join ordering optimization
	// this also rewrites cross products + filters into joins and performs filter pushdowns
	JoinOrderOptimizer optimizer;
	plan = optimizer.Optimize(move(plan));
	// then we extract common subexpressions inside the different operators
	CommonSubExpressionOptimizer cse_optimizer;
	cse_optimizer.VisitOperator(*plan);
	return plan;
}
