#include "duckdb/optimizer/in_clause_rewriter.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> InClauseRewriter::Rewrite(unique_ptr<LogicalOperator> op) {
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

unique_ptr<Expression> InClauseRewriter::VisitReplace(BoundOperatorExpression &expr, unique_ptr<Expression> *expr_ptr) {
	if (expr.type != ExpressionType::COMPARE_IN && expr.type != ExpressionType::COMPARE_NOT_IN) {
		return nullptr;
	}
	D_ASSERT(root);
	auto in_type = expr.children[0]->return_type;
	bool is_regular_in = expr.type == ExpressionType::COMPARE_IN;
	bool all_scalar = true;
	// IN clause with many children: try to generate a mark join that replaces this IN expression
	// we can only do this if the expressions in the expression list are scalar
	for (idx_t i = 1; i < expr.children.size(); i++) {
		D_ASSERT(expr.children[i]->return_type == in_type);
		if (!expr.children[i]->IsFoldable()) {
			// non-scalar expression
			all_scalar = false;
		}
	}
	if (expr.children.size() == 2) {
		// only one child
		// IN: turn into X = 1
		// NOT IN: turn into X <> 1
		return make_unique<BoundComparisonExpression>(is_regular_in ? ExpressionType::COMPARE_EQUAL
		                                                            : ExpressionType::COMPARE_NOTEQUAL,
		                                              move(expr.children[0]), move(expr.children[1]));
	}
	if (expr.children.size() < 6 || !all_scalar) {
		// low amount of children or not all scalar
		// IN: turn into (X = 1 OR X = 2 OR X = 3...)
		// NOT IN: turn into (X <> 1 AND X <> 2 AND X <> 3 ...)
		auto conjunction = make_unique<BoundConjunctionExpression>(is_regular_in ? ExpressionType::CONJUNCTION_OR
		                                                                         : ExpressionType::CONJUNCTION_AND);
		for (idx_t i = 1; i < expr.children.size(); i++) {
			conjunction->children.push_back(make_unique<BoundComparisonExpression>(
			    is_regular_in ? ExpressionType::COMPARE_EQUAL : ExpressionType::COMPARE_NOTEQUAL,
			    expr.children[0]->Copy(), move(expr.children[i])));
		}
		return move(conjunction);
	}
	// IN clause with many constant children
	// generate a mark join that replaces this IN expression
	// first generate a ColumnDataCollection from the set of expressions
	vector<LogicalType> types = {in_type};
	auto collection = make_unique<ColumnDataCollection>(context, types);
	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);

	DataChunk chunk;
	chunk.Initialize(context, types);
	for (idx_t i = 1; i < expr.children.size(); i++) {
		// resolve this expression to a constant
		auto value = ExpressionExecutor::EvaluateScalar(*expr.children[i]);
		idx_t index = chunk.size();
		chunk.SetCardinality(chunk.size() + 1);
		chunk.SetValue(0, index, value);
		if (chunk.size() == STANDARD_VECTOR_SIZE || i + 1 == expr.children.size()) {
			// chunk full: append to chunk collection
			collection->Append(append_state, chunk);
			chunk.Reset();
		}
	}
	// now generate a ChunkGet that scans this collection
	auto chunk_index = optimizer.binder.GenerateTableIndex();
	auto chunk_scan = make_unique<LogicalColumnDataGet>(chunk_index, types, move(collection));

	// then we generate the MARK join with the chunk scan on the RHS
	auto join = make_unique<LogicalComparisonJoin>(JoinType::MARK);
	join->mark_index = chunk_index;
	join->AddChild(move(root));
	join->AddChild(move(chunk_scan));
	// create the JOIN condition
	JoinCondition cond;
	cond.left = move(expr.children[0]);

	cond.right = make_unique<BoundColumnRefExpression>(in_type, ColumnBinding(chunk_index, 0));
	cond.comparison = ExpressionType::COMPARE_EQUAL;
	join->conditions.push_back(move(cond));
	root = move(join);

	// we replace the original subquery with a BoundColumnRefExpression referring to the mark column
	unique_ptr<Expression> result =
	    make_unique<BoundColumnRefExpression>("IN (...)", LogicalType::BOOLEAN, ColumnBinding(chunk_index, 0));
	if (!is_regular_in) {
		// NOT IN: invert
		auto invert = make_unique<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
		invert->children.push_back(move(result));
		result = move(invert);
	}
	return result;
}

} // namespace duckdb
