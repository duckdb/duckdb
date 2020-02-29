#include "duckdb/execution/operator/schema/physical_create_sequence.hpp"
#include "duckdb/catalog/catalog.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateSequence::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	Catalog::GetCatalog(context).CreateSequence(context, info.get());
	state->finished = true;
}
