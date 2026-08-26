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
#include "duckdb/planner/compiler_result.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct ColumnBindingVerificationState;

//! The ColumnBindingResolver resolves ColumnBindings into base tables
//! (table_index, column_index) into physical indices into the DataChunks that
//! are used within the execution engine
class ColumnBindingResolver : public LogicalOperatorVisitor {
public:
	explicit ColumnBindingResolver(bool verify_only = false);

	void VisitOperator(LogicalOperator &op) override;
	static void Verify(ClientContext &context, LogicalOperator &op);
	DUCKDB_API static CompilerResult<VerificationSuccess> VerifyAlways(LogicalOperator &op);

protected:
	vector<ColumnBinding> bindings;
	vector<LogicalType> types;
	bool verify_only;
	optional_ptr<ColumnBindingVerificationState> verification_state;

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	explicit ColumnBindingResolver(ColumnBindingVerificationState &verification_state);
	static bool ResolveOperatorTypes(LogicalOperator &op, ColumnBindingVerificationState &verification_state);
};
} // namespace duckdb
