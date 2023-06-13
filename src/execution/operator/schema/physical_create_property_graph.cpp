#include "duckdb/execution/operator/schema/physical_create_property_graph.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_data.hpp"
#include "../../../../../duckpgq/include/duckpgq_extension.hpp"

namespace duckdb {

class CreatePropertyGraphSourceState : public GlobalSourceState {
public:
	CreatePropertyGraphSourceState() : finished(false) {
	}

	bool finished;
};

PhysicalCreatePropertyGraph::PhysicalCreatePropertyGraph(unique_ptr<CreatePropertyGraphInfo> info,
                                                         idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_PROPERTY_GRAPH, {LogicalType::BIGINT}, estimated_cardinality),
      info(std::move(info)) {
}

unique_ptr<GlobalSourceState> PhysicalCreatePropertyGraph::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CreatePropertyGraphSourceState>();
}

SourceResultType PhysicalCreatePropertyGraph::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
    auto &gstate = input.global_state.Cast<CreatePropertyGraphSourceState>();
    if (gstate.finished) {
        return SourceResultType::FINISHED;
    }

	//! During the binder we already check if the property graph exists
	auto duckpgq_state_entry = context.client.registered_state.find("duckpgq");
	if (duckpgq_state_entry == context.client.registered_state.end()) {
		throw MissingExtensionException("The SQL/PGQ extension has not been loaded");
	}
	auto duckpgq_state = reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());
//	duckpgq_state->registered_property_graphs[info->property_graph_name] = property_graph_nameinfo->Copy();
	return SourceResultType::FINISHED;
}

} // namespace duckdb
