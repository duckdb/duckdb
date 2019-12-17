#include "duckdb/optimizer/optimizer.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/cse_optimizer.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/index_scan.hpp"
#include "duckdb/optimizer/join_order_optimizer.hpp"
#include "duckdb/optimizer/regex_range_filter.hpp"
#include "duckdb/optimizer/rule/list.hpp"
#include "duckdb/optimizer/topn_optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/common_subexpression.hpp"
#include "duckdb/planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

Optimizer::Optimizer(Binder &binder, ClientContext &context) : context(context), binder(binder), rewriter(context) {
	rewriter.rules.push_back(make_unique<ConstantFoldingRule>(rewriter));
	rewriter.rules.push_back(make_unique<DistributivityRule>(rewriter));
	rewriter.rules.push_back(make_unique<ArithmeticSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<CaseSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<ConjunctionSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<ComparisonSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_unique<MoveConstantsRule>(rewriter));

#ifdef DEBUG
	for (auto &rule : rewriter.rules) {
		// root not defined in rule
		assert(rule->root);
	}
#endif
}

namespace duckdb {

class InClauseRewriter : public LogicalOperatorVisitor {
public:
	InClauseRewriter(Optimizer &optimizer) : optimizer(optimizer) {
	}

	Optimizer &optimizer;
	unique_ptr<LogicalOperator> root;

	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op) {
		if (op->children.size() == 1) {
			root = move(op->children[0]);
			VisitOperatorExpressions(*op);
			op->children[0] = move(root);
		}

		for (auto &child : op->children) {
			child = Rewrite(move(child));
		}
		return op;
	}

	unique_ptr<Expression> VisitReplace(BoundOperatorExpression &expr, unique_ptr<Expression> *expr_ptr) override;
};

} // namespace duckdb

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

	context.profiler.StartPhase("in_clause");
	InClauseRewriter rewriter(*this);
	plan = rewriter.Rewrite(move(plan));
	context.profiler.EndPhase();

	// transform ORDER BY + LIMIT to TopN
	context.profiler.StartPhase("top_n");
	TopN topn;
	plan = topn.Optimize(move(plan));
	context.profiler.EndPhase();

	return plan;
}

unique_ptr<Expression> InClauseRewriter::VisitReplace(BoundOperatorExpression &expr, unique_ptr<Expression> *expr_ptr) {
	if (expr.type != ExpressionType::COMPARE_IN) {
		return nullptr;
	}
	if (expr.children[0]->IsFoldable()) {
		// LHS is scalar: we can flatten the entire list
		return nullptr;
	}
	if (expr.children.size() < 6) {
		// not enough children for flattening to be worth it
		return nullptr;
	}
	assert(root);
	auto in_type = expr.children[0]->return_type;
	// IN clause with many children: try to generate a mark join that replaces this IN expression
	// we can only do this if the expressions in the expression list are scalar
	for (index_t i = 1; i < expr.children.size(); i++) {
		assert(expr.children[i]->return_type == in_type);
		if (!expr.children[i]->IsFoldable()) {
			// non-scalar expression
			return nullptr;
		}
	}
	// IN clause with many constant children
	// generate a mark join that replaces this IN expression
	// first generate a ChunkCollection from the set of expressions
	vector<TypeId> types = {in_type};
	auto collection = make_unique<ChunkCollection>();
	DataChunk chunk;
	chunk.Initialize(types);
	for (index_t i = 1; i < expr.children.size(); i++) {
		// reoslve this expression to a constant
		auto value = ExpressionExecutor::EvaluateScalar(*expr.children[i]);
		index_t index = chunk.data[0].count++;
		chunk.data[0].SetValue(index, value);
		if (chunk.data[0].count == STANDARD_VECTOR_SIZE || i + 1 == expr.children.size()) {
			// chunk full: append to chunk collection
			collection->Append(chunk);
			chunk.Reset();
		}
	}
	// now generate a ChunkGet that scans this collection
	auto chunk_index = optimizer.binder.GenerateTableIndex();
	auto chunk_scan = make_unique<LogicalChunkGet>(chunk_index, types, move(collection));

	auto subquery_index = optimizer.binder.GenerateTableIndex();
	auto logical_subquery = make_unique<LogicalSubquery>(move(chunk_scan), subquery_index);

	// then we generate the MARK join with the chunk scan on the RHS
	auto join = make_unique<LogicalComparisonJoin>(JoinType::MARK);
	join->AddChild(move(root));
	join->AddChild(move(logical_subquery));
	// create the JOIN condition
	JoinCondition cond;
	cond.left = move(expr.children[0]);

	cond.right = make_unique<BoundColumnRefExpression>(in_type, ColumnBinding(subquery_index, 0));
	cond.comparison = ExpressionType::COMPARE_EQUAL;
	join->conditions.push_back(move(cond));
	root = move(join);

	// we replace the original subquery with a BoundColumnRefExpression refering to the mark column
	return make_unique<BoundColumnRefExpression>("IN (...)", TypeId::BOOLEAN, ColumnBinding(subquery_index, 0));
}
