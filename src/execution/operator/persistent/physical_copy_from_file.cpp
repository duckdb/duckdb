#include "duckdb/execution/operator/persistent/physical_copy_from_file.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

class PhysicalCopyFromFileOperatorState : public PhysicalOperatorState {
public:
	PhysicalCopyFromFileOperatorState(PhysicalOperator &op) : PhysicalOperatorState(op, nullptr) {}
	//! The global function data
	unique_ptr<GlobalFunctionData> gdata;
};

void PhysicalCopyFromFile::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                            PhysicalOperatorState *state_) {
	auto &state = (PhysicalCopyFromFileOperatorState &)*state_;
	if (!state.gdata) {
		//! initialize the reader
		state.gdata = function.copy_from_initialize(context.client, *info);
	}
	// read a chunk from the reader
	function.copy_from_get_chunk(context, *state.gdata, *info, chunk);
}

unique_ptr<PhysicalOperatorState> PhysicalCopyFromFile::GetOperatorState() {
	return make_unique<PhysicalCopyFromFileOperatorState>(*this);
}

} // namespace duckdb
