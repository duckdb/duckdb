#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/column_binding_map.hpp"

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
