//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/physical_table_function.hpp
//
// Author: Mark Raasveldt
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
	      function(function), function_call(std::move(function_call)) {}

	//! Function to exit
	TableFunctionCatalogEntry *function;
	//! Expressions
	std::unique_ptr<Expression> function_call;

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;

	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;
};

class PhysicalTableFunctionOperatorState : public PhysicalOperatorState {
  public:
	PhysicalTableFunctionOperatorState(ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(nullptr, parent_executor),
	      function_data(nullptr), initialized(false) {}

	void *function_data;
	bool initialized;
};

} // namespace duckdb
