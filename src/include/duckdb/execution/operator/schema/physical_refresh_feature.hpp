//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_refresh_feature.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalRefreshFeature represents a REFRESH FEATURE command
class PhysicalRefreshFeature : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::REFRESH_FEATURE;

public:
	explicit PhysicalRefreshFeature(PhysicalPlan &physical_plan, string feature_name, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::REFRESH_FEATURE, {LogicalType::BIGINT},
	                       estimated_cardinality),
	      feature_name(std::move(feature_name)) {
	}

	string feature_name;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
