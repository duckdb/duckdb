#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

class Optimizer;
class LogicalOperator;

class ProjectionPullup : public LogicalOperatorVisitor {
public:
	//! Run a complete pass, applying all pending binding replacements before returning.
	static void Optimize(Optimizer &optimizer, unique_ptr<LogicalOperator> &op);

private:
	ProjectionPullup(Optimizer &optimizer_p, unique_ptr<LogicalOperator> &root_p,
	                 BindingReplacementGraph &replacements_p)
	    : optimizer(optimizer_p), root(root_p), replacements(replacements_p) {
	}

	Optimizer &optimizer;
	unique_ptr<LogicalOperator> &root;
	//! Borrowed from OptimizeSubtree and shared by nested pullup scopes within the same binding boundary.
	BindingReplacementGraph &replacements;
	vector<reference<LogicalOperator>> parents;
	static void OptimizeSubtree(Optimizer &optimizer, unique_ptr<LogicalOperator> &root,
	                            unique_ptr<LogicalOperator> &op);
	void VisitOperator(unique_ptr<LogicalOperator> &op) override;
	void FlushReplacements();
	optional_ptr<LogicalOperator> FindParent(LogicalOperator &target, LogicalOperator &current);
	void PopParents(const LogicalOperator &op);
	void InsertProjectionBelowOp(unique_ptr<LogicalOperator> &op, unique_ptr<LogicalOperator> &child, bool stop_at_op);
	void CanPullThrough(column_binding_map_t<unique_ptr<Expression>> &projection_map, const LogicalProjection &proj,
	                    bool &can_pull_through);
	void RemoveProjection(unique_ptr<LogicalOperator> &op, LogicalProjection &proj,
	                      vector<ColumnBinding> &proj_bindings);
	void PullUpNonColrefProjection(unique_ptr<LogicalOperator> &op, LogicalProjection &proj,
	                               vector<ColumnBinding> &proj_bindings, idx_t pull_up_to_here);
};

} // namespace duckdb
