//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/physical_prune_columns.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalPruneColumns prunes (removes) columns from its input
class PhysicalPruneColumns : public PhysicalOperator {
  public:
	PhysicalPruneColumns(size_t column_limit)
	    : PhysicalOperator(PhysicalOperatorType::PRUNE_COLUMNS),
	      column_limit(column_limit) {
	}

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent) override;

	size_t column_limit;
};
} // namespace duckdb
