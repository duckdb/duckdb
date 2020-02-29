#include "duckdb/execution/operator/schema/physical_create_view.hpp"
#include "duckdb/catalog/catalog.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateView::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	Catalog::GetCatalog(context).CreateView(context, info.get());
	state->finished = true;
}
