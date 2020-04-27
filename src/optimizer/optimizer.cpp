#include "duckdb/optimizer/optimizer.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/column_lifetime_optimizer.hpp"
#include "duckdb/optimizer/cse_optimizer.hpp"
#include "duckdb/optimizer/expression_heuristics.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/in_clause_rewriter.hpp"
#include "duckdb/optimizer/index_scan.hpp"
#include "duckdb/optimizer/join_order_optimizer.hpp"
#include "duckdb/optimizer/regex_range_filter.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/optimizer/rule/list.hpp"
#include "duckdb/optimizer/topn_optimizer.hpp"
#include "duckdb/planner/binder.hpp"

using namespace duckdb;
using namespace std;

Optimizer::Optimizer(Binder &binder, ClientContext &context) : context(context), binder(binder), rewriter(context) {
	rewriter.rules.push_back(make_unique<ConstantFoldingRule>(rewriter));
	rewriter.rules.push_back(make_unique<DistributivityRule>(rewriter));
	rewriter.rules.push_back(make_unique<ArithmeticSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<CaseSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<ConjunctionSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<DatePartSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<ComparisonSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<MoveConstantsRule>(rewriter));
	rewriter.rules.push_back(make_unique<LikeOptimizationRule>(rewriter));
	rewriter.rules.push_back(make_unique<EmptyNeedleRemovalRule>(rewriter));

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
	context.profiler.StartPhase("expression_rewriter");
	rewriter.Apply(*plan);
	context.profiler.EndPhase();

	// perform filter pushdown
	context.profiler.StartPhase("filter_pushdown");
	FilterPushdown filter_pushdown(*this);
	plan = filter_pushdown.Rewrite(move(plan));
	context.profiler.EndPhase();

	// check if filters match with existing indexes, if true transforms filters to index scans
	context.profiler.StartPhase("index_scan");
	IndexScan index_scan;
	plan = index_scan.Optimize(move(plan));
	context.profiler.EndPhase();

	context.profiler.StartPhase("regex_range");
	RegexRangeFilter regex_opt;
	plan = regex_opt.Rewrite(move(plan));
	context.profiler.EndPhase();

	context.profiler.StartPhase("in_clause");
	InClauseRewriter rewriter(*this);
	plan = rewriter.Rewrite(move(plan));
	context.profiler.EndPhase();

	// then we perform the join ordering optimization
	// this also rewrites cross products + filters into joins and performs filter pushdowns
	context.profiler.StartPhase("join_order");
	JoinOrderOptimizer optimizer;
	plan = optimizer.Optimize(move(plan));
	context.profiler.EndPhase();

	// then we extract common subexpressions inside the different operators
	// context.profiler.StartPhase("common_subexpressions");
	// CommonSubExpressionOptimizer cse_optimizer;
	// cse_optimizer.VisitOperator(*plan);
	// context.profiler.EndPhase();

	context.profiler.StartPhase("unused_columns");
	RemoveUnusedColumns unused(true);
	unused.VisitOperator(*plan);
	context.profiler.EndPhase();

	context.profiler.StartPhase("column_lifetime");
	ColumnLifetimeAnalyzer column_lifetime(true);
	column_lifetime.VisitOperator(*plan);
	context.profiler.EndPhase();

	// transform ORDER BY + LIMIT to TopN
	context.profiler.StartPhase("top_n");
	TopN topn;
	plan = topn.Optimize(move(plan));
	context.profiler.EndPhase();

	// apply simple expression heuristics to get an initial reordering
	context.profiler.StartPhase("reorder_filter_expressions");
	ExpressionHeuristics expression_heuristics(*this);
	plan = expression_heuristics.Rewrite(move(plan));
	context.profiler.EndPhase();

	return plan;
}
