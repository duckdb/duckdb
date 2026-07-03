#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_trigger_info.hpp"

namespace duckdb {

//! PhysicalCreateTrigger represents a CREATE TRIGGER command
class PhysicalCreateTrigger : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_TRIGGER;

public:
	explicit PhysicalCreateTrigger(PhysicalPlan &physical_plan, unique_ptr<CreateTriggerInfo> info,
	                               idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_TRIGGER, {LogicalType::BIGINT},
	                       estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<CreateTriggerInfo> info;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
