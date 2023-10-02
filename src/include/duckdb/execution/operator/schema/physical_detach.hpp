//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_detach.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/detach_info.hpp"

namespace duckdb {

class PhysicalDetach : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::DETACH;

public:
	explicit PhysicalDetach(unique_ptr<DetachInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::DETACH, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<DetachInfo> info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
