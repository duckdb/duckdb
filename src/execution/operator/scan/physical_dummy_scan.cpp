#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"

namespace duckdb {

class DummyScanState : public GlobalSourceState {
public:
	DummyScanState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDummyScan::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<DummyScanState>();
}

void PhysicalDummyScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                LocalSourceState &lstate) const {
	auto &state = (DummyScanState &)gstate;
	if (state.finished) {
		return;
	}
	// return a single row on the first call to the dummy scan
	chunk.SetCardinality(1);
	state.finished = true;
}

} // namespace duckdb
