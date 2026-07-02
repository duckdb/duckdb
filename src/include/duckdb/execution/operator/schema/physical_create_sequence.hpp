//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_sequence.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"

namespace duckdb {

//! PhysicalCreateSequence represents a CREATE SEQUENCE command
class PhysicalCreateSequence : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_SEQUENCE;

public:
	explicit PhysicalCreateSequence(PhysicalPlan &physical_plan, unique_ptr<CreateSequenceInfo> info,
	                                idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_SEQUENCE, {LogicalType::BIGINT},
	                       estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<CreateSequenceInfo> info;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
