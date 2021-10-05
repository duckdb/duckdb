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
	return make_unique<CreateSequenceSourceState>();
}

void PhysicalCreateSequence::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                     LocalSourceState &lstate) const {
	auto &state = (CreateSequenceSourceState &)gstate;
	if (state.finished) {
		return;
	}
	Catalog::GetCatalog(context.client).CreateSequence(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb
