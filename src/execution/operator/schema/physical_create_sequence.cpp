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

SourceResultType PhysicalCreateSequence::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<CreateSequenceSourceState>();
	if (state.finished) {
		return SourceResultType::FINISHED;
	}
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	catalog.CreateSequence(context.client, *info);
	state.finished = true;

	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
