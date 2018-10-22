//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/physical_limit.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

//! PhyisicalLimit represents the LIMIT operator
class PhysicalLimit : public PhysicalOperator {
  public:
	PhysicalLimit(size_t limit, size_t offset)
	    : PhysicalOperator(PhysicalOperatorType::LIMIT), limit(limit),
	      offset(offset) {
	}

	size_t limit;
	size_t offset;

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;

	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;
};

class PhysicalLimitOperatorState : public PhysicalOperatorState {
  public:
	PhysicalLimitOperatorState(PhysicalOperator *child,
	                           size_t current_offset = 0,
	                           ExpressionExecutor *parent_executor = nullptr)
	    : PhysicalOperatorState(child, parent_executor),
	      current_offset(current_offset) {
	}

	size_t current_offset;
};
} // namespace duckdb
