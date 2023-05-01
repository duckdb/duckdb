#include "duckdb/execution/operator/schema/physical_create_view.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateViewSourceState : public GlobalSourceState {
public:
	CreateViewSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateView::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CreateViewSourceState>();
}

SourceResultType PhysicalCreateView::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<CreateViewSourceState>();
	if (state.finished) {
		return SourceResultType::FINISHED;
	}
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	catalog.CreateView(context.client, *info);
	state.finished = true;

	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
