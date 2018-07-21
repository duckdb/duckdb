
#include "execution/executor.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<DuckDBResult> Executor::Execute(unique_ptr<PhysicalOperator> plan) {
	unique_ptr<DuckDBResult> result = make_unique<DuckDBResult>();

	// the chunk and state are used to iterate over the input plan
	DataChunk chunk;
	auto state = plan->GetOperatorState();
	plan->InitializeChunk(chunk);
	// // result->data holds the concatenation of the output columns
	plan->InitializeChunk(result->data);
	result->success = false;
	try {
		// loop until we have retrieved all data
		do {
			plan->GetChunk(chunk, state.get());
			result->data.Append(chunk);
		} while(chunk.count > 0);
		result->success = true;
	} catch (Exception ex) {
		result->error = ex.GetMessage();
	} catch (...) {
		result->error = "UNHANDLED EXCEPTION TYPE THROWN IN PLANNER!";
	}
	if (!result->success) {
		result->data.Clear();
	}
	return move(result);
}
