#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class Optimizer;
class LogicalOperator;

class ProjectionPullup {
public:
	explicit ProjectionPullup(LogicalOperator &root) : root(root) {
	}

	void Optimize(unique_ptr<LogicalOperator> &op);
	void PopParents(const LogicalOperator &op);

private:
	LogicalOperator &root;
	vector<reference<LogicalOperator>> parents;
};

} // namespace duckdb
