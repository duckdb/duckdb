//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/remove_useless_projections.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

//! The RemoveUselessProjections Optimizer traverses the logical operator tree and removes all projections that just
class RemoveUselessProjections : LogicalOperatorVisitor {
public:
	RemoveUselessProjections() : first_projection(true) {
	}
	unique_ptr<LogicalOperator> RemoveProjections(unique_ptr<LogicalOperator> plan);
	unique_ptr<LogicalOperator> RemoveProjectionsChildren(unique_ptr<LogicalOperator> plan);
	void ReplaceBindings(LogicalOperator &plan);

private:
	bool first_projection;
	ColumnBindingReplacer replacer;
};

} // namespace duckdb
