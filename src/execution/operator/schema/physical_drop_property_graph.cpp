#include "duckdb/execution/operator/schema/physical_drop_property_graph.hpp"
#include "duckdb/main/client_data.hpp"
#include "../../../../../duckpgq/include/duckpgq_extension.hpp"


namespace duckdb {

class DropPropertyGraphSourceState : public GlobalSourceState {
public:
	DropPropertyGraphSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDropPropertyGraph::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<DropPropertyGraphSourceState>();
}

SourceResultType PhysicalDropPropertyGraph::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
    auto &gstate = input.global_state.Cast<CreatePropertyGraphSourceState>();
    if (gstate.finished) {
		return SourceResultType::FINISHED;
	}

	//! During the binder we already check if the property graph exists
	auto sqlpgq_state_entry = context.client.registered_state.find("sqlpgq");
	if (sqlpgq_state_entry == context.client.registered_state.end()) {
		throw MissingExtensionException("The SQL/PGQ extension has not been loaded");
	}
	auto duckpgq_state = reinterpret_cast<DuckPGQState *>(sqlpgq_state_entry->second.get());
	duckpgq_state->registered_property_graphs.erase(info->name);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
