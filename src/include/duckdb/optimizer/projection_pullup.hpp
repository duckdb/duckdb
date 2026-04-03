#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

class Optimizer;
class LogicalOperator;

class ProjectionPullup {
public:
	explicit ProjectionPullup(Optimizer &optimizer_p, unique_ptr<LogicalOperator> &root)
	    : optimizer(optimizer_p), root(root) {
	}

	void Optimize(unique_ptr<LogicalOperator> &op);
	void PopParents(const LogicalOperator &op);
	void InsertProjectionBelowOp(unique_ptr<LogicalOperator> &op, unique_ptr<LogicalOperator> &child, bool stop_at_op);

private:
	Optimizer &optimizer;
	unique_ptr<LogicalOperator> &root;
	vector<reference<LogicalOperator>> parents;
	optional_ptr<LogicalOperator> FindParent(LogicalOperator &target, LogicalOperator &current);
};

} // namespace duckdb
