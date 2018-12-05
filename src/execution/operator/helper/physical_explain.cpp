#include "execution/operator/helper/physical_explain.hpp"

using namespace duckdb;
using namespace std;

void PhysicalExplain::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	assert(keys.size() < STANDARD_VECTOR_SIZE);

	chunk.data[0].count = chunk.data[1].count = keys.size();
	for (size_t i = 0; i < keys.size(); i++) {
		chunk.data[0].SetValue(i, Value(keys[i]));
		chunk.data[1].SetValue(i, Value(values[i]));
	}
	state->finished = true;
}
