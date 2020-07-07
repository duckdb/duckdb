#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"

using namespace std;

namespace duckdb {

void PhysicalDummyScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	state->finished = true;
	// return a single row on the first call to the dummy scan
	chunk.SetCardinality(1);
}

}
