#include "execution/executor.hpp"

using namespace duckdb;
using namespace std;

ChunkCollection Executor::Execute(ClientContext &context, unique_ptr<PhysicalOperator> plan) {
	ChunkCollection result;
	// the chunk and state are used to iterate over the input plan
	auto state = plan->GetOperatorState(nullptr);

	result.types = plan->GetTypes();

	// loop until we have retrieved all data
	unique_ptr<DataChunk> chunk;
	do {
		chunk = make_unique<DataChunk>();
		plan->InitializeChunk(*chunk.get());
		plan->GetChunk(context, *chunk, state.get());
		result.Append(*chunk);
	} while (chunk->size() > 0);
	return result;
}
