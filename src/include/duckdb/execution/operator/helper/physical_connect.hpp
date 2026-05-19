//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_connect.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/connect_info.hpp"

namespace duckdb {

//! PhysicalConnect routes subsequent SQL on this ClientContext to the named AttachedDatabase via
//! Catalog::ExecuteSQL. The case-insensitive name "LOCAL" unbinds the routing.
class PhysicalConnect : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CONNECT;

public:
	PhysicalConnect(PhysicalPlan &physical_plan, unique_ptr<ConnectInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::CONNECT, {LogicalType::BOOLEAN}, estimated_cardinality),
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
