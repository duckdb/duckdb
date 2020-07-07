#include "duckdb/execution/operator/schema/physical_create_view.hpp"
#include "duckdb/catalog/catalog.hpp"

using namespace std;

namespace duckdb {

void PhysicalCreateView::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	Catalog::GetCatalog(context.client).CreateView(context.client, info.get());
	state->finished = true;
}

}
