#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_feature_info.hpp"

namespace duckdb {

//! PhysicalCreateFeature represents a CREATE FEATURE command. It only registers the feature metadata
//! (catalog entry + resolver view); no version table is materialized until the first REFRESH FEATURE.
class PhysicalCreateFeature : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_FEATURE;

public:
	explicit PhysicalCreateFeature(PhysicalPlan &physical_plan, unique_ptr<CreateFeatureInfo> info,
	                               idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_FEATURE, {LogicalType::VARCHAR},
	                       estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<CreateFeatureInfo> info;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
