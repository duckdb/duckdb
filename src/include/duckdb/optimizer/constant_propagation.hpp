//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/constant_propagation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {
class ClientContext;
class Expression;
class LogicalOperator;

//! Propagates exact constant outputs without changing the operators that produce them.
class ConstantPropagation {
public:
	explicit ConstantPropagation(ClientContext &context);

	void Optimize(LogicalOperator &op);

private:
	using ConstantMap = column_binding_map_t<Value>;

	ConstantMap Propagate(LogicalOperator &op);
	void RewriteExpressions(LogicalOperator &op, const ConstantMap &constants);
	bool RewriteExpression(unique_ptr<Expression> &expr, const ConstantMap &constants);
	static ConstantMap ExtractConstants(TableIndex table_index, const vector<unique_ptr<Expression>> &expressions);
	static ConstantMap GetOutputConstants(LogicalOperator &op, const ConstantMap &constants);

private:
	ClientContext &context;
};

} // namespace duckdb
