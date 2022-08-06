#include "duckdb/execution/operator/persistent/physical_delete.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/transaction.hpp"

#include "duckdb/common/atomic.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class DeleteGlobalState : public GlobalSinkState {
public:
	explicit DeleteGlobalState(Allocator &allocator)
	    : deleted_count(0), return_chunk_collection(allocator), returned_chunk_count(0) {
	}

	mutex delete_lock;
	idx_t deleted_count;
	ChunkCollection return_chunk_collection;
	idx_t returned_chunk_count;
};

class DeleteLocalState : public LocalSinkState {
public:
	DeleteLocalState(Allocator &allocator, const vector<LogicalType> &table_types) {
		delete_chunk.Initialize(allocator, table_types);
	}
	DataChunk delete_chunk;
};

SinkResultType PhysicalDelete::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                    DataChunk &input) const {
	auto &gstate = (DeleteGlobalState &)state;
	auto &ustate = (DeleteLocalState &)lstate;

	// get rows and
	auto &transaction = Transaction::GetTransaction(context.client);
	auto &row_identifiers = input.data[row_id_index];

	vector<column_t> column_ids;
	for (idx_t i = 0; i < table.column_definitions.size(); i++) {
		column_ids.emplace_back(i);
	};
	auto cfs = ColumnFetchState();

	lock_guard<mutex> delete_guard(gstate.delete_lock);
	if (return_chunk) {
		row_identifiers.Flatten(input.size());
		table.Fetch(transaction, ustate.delete_chunk, column_ids, row_identifiers, input.size(), cfs);
		gstate.return_chunk_collection.Append(ustate.delete_chunk);
	}
	gstate.deleted_count += table.Delete(tableref, context.client, row_identifiers, input.size());

	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<DeleteGlobalState>(Allocator::Get(context));
}

unique_ptr<LocalSinkState> PhysicalDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<DeleteLocalState>(Allocator::Get(context.client), table.GetTypes());
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

	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(g.deleted_count));
		state.finished = true;
	}

	idx_t chunk_return = g.returned_chunk_count;
	if (chunk_return >= g.return_chunk_collection.Chunks().size()) {
		return;
	}
	chunk.Reference(g.return_chunk_collection.GetChunk(chunk_return));
	chunk.SetCardinality((g.return_chunk_collection.GetChunk(chunk_return)).size());
	g.returned_chunk_count += 1;
	if (g.returned_chunk_count >= g.return_chunk_collection.Chunks().size()) {
		state.finished = true;
	}
}

} // namespace duckdb
