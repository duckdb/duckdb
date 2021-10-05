#include "duckdb/execution/operator/persistent/physical_delete.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

#include "duckdb/common/atomic.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class DeleteGlobalState : public GlobalSinkState {
public:
	DeleteGlobalState() : deleted_count(0) {
	}

	mutex delete_lock;
	idx_t deleted_count;
};

SinkResultType PhysicalDelete::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                    DataChunk &input) const {
	auto &gstate = (DeleteGlobalState &)state;

	// delete data in the base table
	// the row ids are given to us as the last column of the child chunk
	lock_guard<mutex> delete_guard(gstate.delete_lock);
	gstate.deleted_count += table.Delete(tableref, context.client, input.data[row_id_index], input.size());
	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<DeleteGlobalState>();
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class DeleteSourceState : public GlobalSourceState {
public:
	DeleteSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<DeleteSourceState>();
}

void PhysicalDelete::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &state = (DeleteSourceState &)gstate;
	auto &g = (DeleteGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(g.deleted_count));
	state.finished = true;
}

} // namespace duckdb
