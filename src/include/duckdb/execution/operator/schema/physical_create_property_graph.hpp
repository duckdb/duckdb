#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"

namespace duckdb {

class PhysicalCreatePropertyGraph : public PhysicalOperator {
public:
	PhysicalCreatePropertyGraph(unique_ptr<CreatePropertyGraphInfo> info, idx_t estimated_cardinality);

	unique_ptr<CreatePropertyGraphInfo> info;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;
};
} // namespace duckdb
