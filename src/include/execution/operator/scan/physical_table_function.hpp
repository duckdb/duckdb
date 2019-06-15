//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/scan/physical_table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include "function/function.hpp"
#include "storage/data_table.hpp"

namespace duckdb {

//! Represents a scan of a base table
class PhysicalTableFunction : public PhysicalOperator {
public:
	PhysicalTableFunction(LogicalOperator &op, TableFunctionCatalogEntry *function,
	                      vector<unique_ptr<Expression>> parameters)
	    : PhysicalOperator(PhysicalOperatorType::TABLE_FUNCTION, op.types), function(function),
	      parameters(move(parameters)) {
	}

	//! Function to call
	TableFunctionCatalogEntry *function;
	//! Expressions
	vector<unique_ptr<Expression>> parameters;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalTableFunctionOperatorState : public PhysicalOperatorState {
public:
	PhysicalTableFunctionOperatorState() : PhysicalOperatorState(nullptr), initialized(false) {
	}

	unique_ptr<FunctionData> function_data;
	bool initialized;
};

} // namespace duckdb
