//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_unnest.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! PhysicalWindow implements window functions
class PhysicalTableInOutFunction : public PhysicalOperator {
public:
	PhysicalTableInOutFunction(vector<LogicalType> types, TableFunction function_p,
	                           unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p,
	                           idx_t estimated_cardinality);

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

private:
	//! The table function
	TableFunction function;
	//! Bind data of the function
	unique_ptr<FunctionData> bind_data;

	vector<column_t> column_ids;
};

} // namespace duckdb
