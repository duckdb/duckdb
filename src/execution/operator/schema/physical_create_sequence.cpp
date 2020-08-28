#include "duckdb/execution/operator/schema/physical_create_sequence.hpp"
#include "duckdb/catalog/catalog.hpp"

using namespace std;

namespace duckdb {

void PhysicalCreateSequence::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                              PhysicalOperatorState *state) {
	Catalog::GetCatalog(context.client).CreateSequence(context.client, info.get());
	state->finished = true;
}

} // namespace duckdb
