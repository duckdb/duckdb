#include "optimizer/optimizer.hpp"

#include "optimizer/join_order_optimizer.hpp"
#include "optimizer/obsolete_filter_rewriter.hpp"
#include "optimizer/rule/list.hpp"
#include "optimizer/subquery_rewriter.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

Optimizer::Optimizer(ClientContext &client_context, BindContext &context) : context(context), rewriter(client_context) {
	rewriter.rules.push_back(make_unique<ConstantFoldingRule>(rewriter));
	rewriter.rules.push_back(make_unique<DistributivityRule>(rewriter));
	rewriter.rules.push_back(make_unique<ArithmeticSimplificationRule>(rewriter));

#ifdef DEBUG
	for (auto &rule : rewriter.rules) {
		// root not defined in rule
		assert(rule->root);
	}
#endif
}

class OptimizeSubqueries : public LogicalOperatorVisitor {
public:
	using LogicalOperatorVisitor::Visit;
	unique_ptr<Expression> Visit(SubqueryExpression &subquery) override {
		// we perform join reordering within the subquery expression
		JoinOrderOptimizer optimizer;
		subquery.op = optimizer.Optimize(move(subquery.op));
		return nullptr;
	}
};

unique_ptr<LogicalOperator> Optimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	// first we perform expression rewrites using the ExpressionRewriter
	// this does not change the logical plan structure, but only simplifies the expression trees
	rewriter.Apply(*plan);
	// now perform obsolete filter removal
	ObsoleteFilterRewriter obsolete_filter;
	plan = obsolete_filter.Rewrite(move(plan));
	// then we perform the join ordering optimization
	// this also rewrites cross products + filters into joins and performs filter pushdowns
	JoinOrderOptimizer optimizer;
	plan = optimizer.Optimize(move(plan));
	// perform join order optimization in subqueries as well
	OptimizeSubqueries opt;
	plan->Accept(&opt);
	// finally we rewrite subqueries
	SubqueryRewriter subquery_rewriter(context);
	return subquery_rewriter.Rewrite(move(plan));
}
