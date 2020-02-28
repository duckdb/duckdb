#include "duckdb/execution/operator/schema/physical_create_view.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateView::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	context.catalog.CreateView(context, info.get());
	state->finished = true;
}
