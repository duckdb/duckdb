#include "duckdb/execution/operator/schema/physical_drop_property_graph.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

class DropPropertyGraphSourceState : public GlobalSourceState {
public:
	DropPropertyGraphSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDropPropertyGraph::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<DropPropertyGraphSourceState>();
}

void PhysicalDropPropertyGraph::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                        LocalSourceState &lstate) const {
	auto &state = (DropPropertyGraphSourceState &)gstate;
	if (state.finished) {
		return;
	}

	//! During the binder we already check if the property graph exists
	auto sqlpgq_state_entry = context.client.registered_state.find("sqlpgq");
	if (sqlpgq_state_entry == context.client.registered_state.end()) {
		throw MissingExtensionException("The SQL/PGQ extension has not been loaded");
	}
	auto sqlpgq_state = reinterpret_cast<SQLPGQContext *>(sqlpgq_state_entry->second.get());
	sqlpgq_state->registered_property_graphs.erase(info->name);
	state.finished = true;
}

} // namespace duckdb
