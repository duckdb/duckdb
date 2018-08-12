
#include "execution/executor.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<DuckDBResult> Executor::Execute(unique_ptr<PhysicalOperator> plan) {
	unique_ptr<DuckDBResult> result = make_unique<DuckDBResult>();

	// the chunk and state are used to iterate over the input plan
	auto state = plan->GetOperatorState(nullptr);
	// // result->data holds the concatenation of the output columns
	result->success = false;
	// initialize the result
	DataChunk dummy_chunk;
	plan->InitializeChunk(dummy_chunk);
	result->Initialize(dummy_chunk);

	try {
		DataChunk chunk;
		// loop until we have retrieved all data
		while (true) {
			auto chunk = make_unique<DataChunk>();
			plan->InitializeChunk(*chunk.get());
			plan->GetChunk(*chunk.get(), state.get());
			if (chunk->count == 0)
				break;
			result->count += chunk->count;
			chunk->ForceOwnership();
			result->data.push_back(move(chunk));
		}
		result->success = true;
	} catch (Exception ex) {
		result->error = ex.GetMessage();
	} catch (...) {
		result->error = "UNHANDLED EXCEPTION TYPE THROWN IN PLANNER!";
	}
	if (!result->success) {
		result->data.clear();
	}
	return move(result);
}
