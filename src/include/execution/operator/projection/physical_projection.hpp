//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/projection/physical_projection.hpp
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalProjection : public PhysicalOperator {
  public:
	PhysicalProjection(std::vector<std::unique_ptr<Expression>> select_list)
	    : PhysicalOperator(PhysicalOperatorType::PROJECTION),
	      select_list(move(select_list)) {
	}

	std::vector<TypeId> GetTypes() override;

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	std::vector<std::unique_ptr<Expression>> select_list;
};

} // namespace duckdb
