#include "duckdb/execution/operator/schema/physical_create_view.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

void PhysicalCreateView::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                          PhysicalOperatorState *state) const {
	Catalog::GetCatalog(context.client).CreateView(context.client, info.get());
	state->finished = true;
}

} // namespace duckdb
