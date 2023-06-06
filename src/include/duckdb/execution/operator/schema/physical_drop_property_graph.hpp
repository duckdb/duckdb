#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/drop_property_graph_info.hpp"

namespace duckdb {

class PhysicalDropPropertyGraph : public PhysicalOperator {
public:
	explicit PhysicalDropPropertyGraph(unique_ptr<DropPropertyGraphInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::DROP_PROPERTY_GRAPH, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<DropPropertyGraphInfo> info;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;
};

} // namespace duckdb
