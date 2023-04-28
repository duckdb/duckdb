#include "duckdb/execution/operator/schema/physical_create_sequence.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateSequenceSourceState : public GlobalSourceState {
public:
	CreateSequenceSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateSequence::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CreateSequenceSourceState>();
}

void PhysicalCreateSequence::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                     LocalSourceState &lstate) const {
	auto &state = gstate.Cast<CreateSequenceSourceState>();
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	catalog.CreateSequence(context.client, *info);
	state.finished = true;
}

} // namespace duckdb
