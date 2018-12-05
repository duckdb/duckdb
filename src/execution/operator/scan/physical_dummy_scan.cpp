#include "execution/operator/scan/physical_dummy_scan.hpp"

using namespace duckdb;
using namespace std;

void PhysicalDummyScan::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	state->finished = true;
	chunk.data[0].count = 1;
}
