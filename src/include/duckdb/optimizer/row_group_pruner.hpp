//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/row_group_pruner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {
class LogicalGet;
class LogicalOperator;

struct RowGroupPrunerParameters {
	RowGroupPrunerParameters(unique_ptr<RowGroupOrderOptions> options_p, LogicalGet &logical_get_p)
	    : options(std::move(options_p)), logical_get(logical_get_p) {
	}

	unique_ptr<RowGroupOrderOptions> options;
	reference<LogicalGet> logical_get;
};

class RowGroupPruner {
public:
	explicit RowGroupPruner(ClientContext &context);

	//! Reorder and try to prune row groups in queries with LIMIT or simple aggregates
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether we can perform the optimization on this operator
	unique_ptr<RowGroupPrunerParameters> CanOptimize(LogicalOperator &op,
	                                                 optional_ptr<ClientContext> context = nullptr);

private:
	ClientContext &context;
};

} // namespace duckdb
