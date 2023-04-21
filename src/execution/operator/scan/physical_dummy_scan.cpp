#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"

namespace duckdb {

class DummyScanState : public GlobalSourceState {
public:
	DummyScanState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDummyScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<DummyScanState>();
}

SourceResultType PhysicalDummyScan::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &state = (DummyScanState &)input.global_state;
	if (state.finished) {
		return SourceResultType::FINISHED;
	}
	// return a single row on the first call to the dummy scan
	chunk.SetCardinality(1);
	state.finished = true;

	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
