//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/mark_join_simplifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ClientContext;
class LogicalFilter;

//! Convert filtered MARK joins using the current plan's marker uses.
class MarkJoinSimplifier {
public:
	explicit MarkJoinSimplifier(ClientContext &context);
	void Optimize(unique_ptr<LogicalOperator> &plan);

private:
	void CountBindings(LogicalOperator &op);
	void Rewrite(unique_ptr<LogicalOperator> &op);
	bool TrySimplify(LogicalFilter &filter);

private:
	ClientContext &context;
	column_binding_map_t<idx_t> binding_uses;
};

} // namespace duckdb
