//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_empty_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalEmptyResult : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EMPTY_RESULT;

public:
	explicit PhysicalEmptyResult(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EMPTY_RESULT, std::move(types), estimated_cardinality) {
	}

public:
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};
} // namespace duckdb
