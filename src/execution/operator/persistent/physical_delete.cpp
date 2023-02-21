#include "duckdb/execution/operator/persistent/physical_delete.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include "duckdb/common/atomic.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class DeleteGlobalState : public GlobalSinkState {
public:
	explicit DeleteGlobalState(ClientContext &context, const vector<LogicalType> &return_types)
	    : deleted_count(0), return_collection(context, return_types) {
	}

	mutex delete_lock;
	idx_t deleted_count;
	ColumnDataCollection return_collection;
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
	auto &transaction = DuckTransaction::Get(context.client, table.db);
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
		gstate.return_collection.Append(ustate.delete_chunk);
	}
	gstate.deleted_count += table.Delete(tableref, context.client, row_identifiers, input.size());

	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<DeleteGlobalState>(context, GetTypes());
}

unique_ptr<LocalSinkState> PhysicalDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<DeleteLocalState>(Allocator::Get(context.client), table.GetTypes());
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class DeleteSourceState : public GlobalSourceState {
public:
	explicit DeleteSourceState(const PhysicalDelete &op) : finished(false) {
		if (op.return_chunk) {
			D_ASSERT(op.sink_state);
			auto &g = (DeleteGlobalState &)*op.sink_state;
			g.return_collection.InitializeScan(scan_state);
		}
	}

	ColumnDataScanState scan_state;
	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<DeleteSourceState>(*this);
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
		return;
	}

	g.return_collection.Scan(state.scan_state, chunk);
}

} // namespace duckdb
