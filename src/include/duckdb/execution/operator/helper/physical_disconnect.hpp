//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_disconnect.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/disconnect_info.hpp"

namespace duckdb {

//! PhysicalDisconnect clears any active CONNECT binding on the ClientContext.
class PhysicalDisconnect : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::DISCONNECT;

public:
	PhysicalDisconnect(PhysicalPlan &physical_plan, unique_ptr<DisconnectInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::DISCONNECT, {LogicalType::BOOLEAN},
	                       estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<DisconnectInfo> info;

public:
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
