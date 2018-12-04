//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/data_modification/physical_update.hpp
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! Physically update data in a table
class PhysicalUpdate : public PhysicalOperator {
  public:
	PhysicalUpdate(TableCatalogEntry &tableref, DataTable &table,
	               std::vector<column_t> columns,
	               std::vector<std::unique_ptr<Expression>> expressions)
	    : PhysicalOperator(PhysicalOperatorType::UPDATE), tableref(tableref),
	      table(table), columns(columns), expressions(std::move(expressions)) {
	}

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	TableCatalogEntry &tableref;
	DataTable &table;
	std::vector<column_t> columns;
	std::vector<std::unique_ptr<Expression>> expressions;
};

} // namespace duckdb
