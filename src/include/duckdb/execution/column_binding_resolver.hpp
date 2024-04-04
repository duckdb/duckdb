//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/column_binding_resolver.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! The ColumnBindingResolver resolves ColumnBindings into base tables
//! (table_index, column_index) into physical indices into the DataChunks that
//! are used within the execution engine
class ColumnBindingResolver : public LogicalOperatorVisitor {
public:
	explicit ColumnBindingResolver(bool verify_only = false);

	void VisitOperator(LogicalOperator &op) override;
	static void Verify(LogicalOperator &op);

protected:
	vector<ColumnBinding> bindings;
	bool verify_only;

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	static unordered_set<idx_t> VerifyInternal(LogicalOperator &op);
};
} // namespace duckdb
