//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_connect_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/connect_info.hpp"

namespace duckdb {

//! No-op placeholder for `CONNECT <target> EXECUTE`. The actual dispatch of the following peel
//! is driven by the Query() loop; this physical op exists so the statement flows through the
//! prepared-statement pipeline without a "no plan generated" failure.
class PhysicalConnectExecute : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CONNECT_EXECUTE;

public:
	PhysicalConnectExecute(PhysicalPlan &physical_plan, unique_ptr<ConnectInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::CONNECT_EXECUTE, {LogicalType::BOOLEAN},
	                       estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<ConnectInfo> info;

public:
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
