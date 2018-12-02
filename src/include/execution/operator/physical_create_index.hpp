//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// execution/operator/physical_create_index.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include <fstream>

namespace duckdb {

//! Physically CREATE INDEX statement
class PhysicalCreateIndex : public PhysicalOperator {
  public:
	PhysicalCreateIndex(TableCatalogEntry &table,
	                    std::vector<column_t> column_ids,
	                    std::vector<std::unique_ptr<Expression>> expressions,
	                    std::unique_ptr<CreateIndexInformation> info)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_INDEX), table(table),
	      column_ids(column_ids), expressions(std::move(expressions)),
	      info(std::move(info)) {
	}

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;
	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	//! The table to create the index for
	TableCatalogEntry &table;
	//! The list of column IDs required for the index
	std::vector<column_t> column_ids;
	//! Set of expressions to index by
	std::vector<std::unique_ptr<Expression>> expressions;
	// Info for index creation
	std::unique_ptr<CreateIndexInformation> info;
};
} // namespace duckdb
