//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

//! Represents a scan of a base table
class PhysicalTableFunction : public PhysicalOperator {
public:
	PhysicalTableFunction(vector<TypeId> types, TableFunctionCatalogEntry *function,
	                      vector<Value> parameters)
	    : PhysicalOperator(PhysicalOperatorType::TABLE_FUNCTION, types), function(function),
	      parameters(move(parameters)) {
	}

	//! Function to call
	TableFunctionCatalogEntry *function;
	//! Parameters
	vector<Value> parameters;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

} // namespace duckdb
