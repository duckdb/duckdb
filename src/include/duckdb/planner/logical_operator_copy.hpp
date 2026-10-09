#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

//! Private state for an in-process copy. Never attached to a persistent serializer.
class LogicalOperatorCopyState {
public:
	struct Scan {
		BoundTableFunction function;
		unique_ptr<FunctionData> bind_data;
		virtual_column_map_t virtual_columns;
	};

	idx_t CopyScan(const LogicalGet &get);
	unique_ptr<Scan> TakeScan(idx_t index);
	void VerifyConsumed() const;
	static void Validate(const LogicalOperator &op);
	static void CopyCardinality(const LogicalOperator &source, LogicalOperator &target);

private:
	vector<unique_ptr<Scan>> scans;
};

} // namespace duckdb
