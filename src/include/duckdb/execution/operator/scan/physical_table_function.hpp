//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

//! Represents a scan of a base table
class PhysicalTableFunction : public PhysicalOperator {
public:
	PhysicalTableFunction(vector<TypeId> types, TableFunction function, unique_ptr<FunctionData> bind_data,
	                      vector<Value> parameters)
	    : PhysicalOperator(PhysicalOperatorType::TABLE_FUNCTION, types), function(move(function)), bind_data(move(bind_data)),
	      parameters(move(parameters)) {
	}

	//! Function to call
	TableFunction function;
	//! The bind data
	unique_ptr<FunctionData> bind_data;
	//! Parameters
	vector<Value> parameters;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	string ExtraRenderInformation() const override;
};

} // namespace duckdb
