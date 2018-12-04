//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/scan/physical_table_function.hpp
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

//! Represents a scan of a base table
class PhysicalTableFunction : public PhysicalOperator {
  public:
	PhysicalTableFunction(TableFunctionCatalogEntry *function,
	                      std::unique_ptr<Expression> function_call)
	    : PhysicalOperator(PhysicalOperatorType::TABLE_FUNCTION),
	      function(function), function_call(std::move(function_call)) {
	}

	//! Function to call
	TableFunctionCatalogEntry *function;
	//! Expressions
	std::unique_ptr<Expression> function_call;

	std::vector<TypeId> GetTypes() override;

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;
};

class PhysicalTableFunctionOperatorState : public PhysicalOperatorState {
  public:
	PhysicalTableFunctionOperatorState(ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(nullptr, parent_executor), initialized(false) {
	}

	std::unique_ptr<TableFunctionData> function_data;
	bool initialized;
};

} // namespace duckdb
