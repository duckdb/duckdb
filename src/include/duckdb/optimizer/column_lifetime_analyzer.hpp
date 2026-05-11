//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/column_lifetime_analyzer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

class Optimizer;
class BoundColumnRefExpression;
class LogicalGet;

//! The ColumnLifetimeAnalyzer optimizer traverses the logical operator tree and ensures that columns are removed from
//! the plan when no longer required
class ColumnLifetimeAnalyzer : public LogicalOperatorVisitor {
public:
	ColumnLifetimeAnalyzer(Optimizer &optimizer_p, LogicalOperator &root_p, bool is_root = false)
	    : optimizer(optimizer_p), root(root_p), everything_referenced(is_root),
	      owned_dead_gets(make_uniq<vector<reference<LogicalGet>>>()), dead_gets(*owned_dead_gets) {
	}

	void VisitOperator(LogicalOperator &op) override;

	//! Apply the dead-column prunes recorded during the walk. Must be called by the top-level analyzer after
	//! VisitOperator finishes; rewrites column_ids/projection_ids/table_filters and fixes upstream bindings in
	//! a single pass.
	void FinalizeDeadColumnPrunes();

public:
	static void ExtractColumnBindings(const Expression &expr, vector<ColumnBinding> &bindings);

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundReferenceExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	//! Sub-analyzer constructor: shares the parent's dead-get queue so prunes recorded in nested traversals are
	//! finalized together with the top-level analyzer.
	ColumnLifetimeAnalyzer(Optimizer &optimizer_p, LogicalOperator &root_p, bool is_root,
	                       vector<reference<LogicalGet>> &shared_dead_gets)
	    : optimizer(optimizer_p), root(root_p), everything_referenced(is_root), dead_gets(shared_dead_gets) {
	}

	Optimizer &optimizer;
	LogicalOperator &root;
	//! Whether or not all the columns are referenced. This happens in the case of the root expression (because the
	//! output implicitly refers all the columns below it)
	bool everything_referenced;
	//! The set of column references
	column_binding_set_t column_references;
	//! Owned only by the top-level analyzer; sub-analyzers reference parent's vector via dead_gets.
	unique_ptr<vector<reference<LogicalGet>>> owned_dead_gets;
	//! Gets queued for dead-filter-column pruning. Mutations are deferred until the walk finishes so that
	//! projection_map generation in joins sees consistent (un-pruned) bindings throughout.
	vector<reference<LogicalGet>> &dead_gets;

private:
	void VisitOperatorInternal(LogicalOperator &op);
	void StandardVisitOperator(LogicalOperator &op);
	void ExtractUnusedColumnBindings(const vector<ColumnBinding> &bindings, column_binding_set_t &unused_bindings);
	static void GenerateProjectionMap(vector<ColumnBinding> bindings, column_binding_set_t &unused_bindings,
	                                  vector<ProjectionIndex> &map);
	//! Record a Get for dead-column pruning if it has columns that are neither projected upstream nor referenced
	//! by a table filter. Pruning runs after StatisticsPropagator removes always-true filters via table-level stats.
	void RecordDeadColumnGet(LogicalGet &get);
	void Verify(LogicalOperator &op);
	void AddVerificationProjection(unique_ptr<LogicalOperator> &child);
};
} // namespace duckdb
