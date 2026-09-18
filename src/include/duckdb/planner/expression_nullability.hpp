//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_nullability.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class Expression;
class LogicalCTE;
class LogicalOperator;

//! Conservatively proves that an expression cannot be NULL at a logical operator's output.
class NotNullExpressionAnalyzer {
public:
	explicit NotNullExpressionAnalyzer(ClientContext &context, optional_ptr<LogicalOperator> plan_root = nullptr);

	bool IsNotNull(LogicalOperator &op, const Expression &expr);

private:
	bool IsNotNull(LogicalOperator &op, const Expression &expr, vector<TableIndex> &seen_ctes);
	optional_ptr<LogicalCTE> FindCTE(TableIndex cte_index);

private:
	ClientContext &context;
	optional_ptr<LogicalOperator> plan_root;
};

} // namespace duckdb
