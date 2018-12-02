//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// execution/operator/physical_filter.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalFilter represents a filter operator. It removes non-matching tupels
//! from the result. Note that it does not physically change the data, it only
//! adds a selection vector to the chunk.
class PhysicalFilter : public PhysicalOperator {
  public:
	PhysicalFilter(std::vector<std::unique_ptr<Expression>> select_list)
	    : PhysicalOperator(PhysicalOperatorType::FILTER),
	      expressions(std::move(select_list)) {
	}

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent) override;

	std::string ExtraRenderInformation() override;

	std::vector<std::unique_ptr<Expression>> expressions;
};
} // namespace duckdb
