#include "duckdb/execution/operator/persistent/physical_delete.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

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

SinkResultType PhysicalDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<DeleteGlobalState>();
	auto &ustate = input.local_state.Cast<DeleteLocalState>();

	// get rows and
	auto &transaction = DuckTransaction::Get(context.client, table.db);
	auto &row_identifiers = chunk.data[row_id_index];

	vector<column_t> column_ids;
	for (idx_t i = 0; i < table.column_definitions.size(); i++) {
		column_ids.emplace_back(i);
	};
	auto cfs = ColumnFetchState();

	lock_guard<mutex> delete_guard(gstate.delete_lock);
	if (return_chunk) {
		row_identifiers.Flatten(chunk.size());
		table.Fetch(transaction, ustate.delete_chunk, column_ids, row_identifiers, chunk.size(), cfs);
		gstate.return_collection.Append(ustate.delete_chunk);
	}
	gstate.deleted_count += table.Delete(tableref, context.client, row_identifiers, chunk.size());

	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DeleteGlobalState>(context, GetTypes());
}

unique_ptr<LocalSinkState> PhysicalDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<DeleteLocalState>(Allocator::Get(context.client), table.GetTypes());
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class DeleteSourceState : public GlobalSourceState {
public:
	explicit DeleteSourceState(const PhysicalDelete &op) {
		if (op.return_chunk) {
			D_ASSERT(op.sink_state);
			auto &g = op.sink_state->Cast<DeleteGlobalState>();
			g.return_collection.InitializeScan(scan_state);
		}
	}

	ColumnDataScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<DeleteSourceState>(*this);
}

SourceResultType PhysicalDelete::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<DeleteSourceState>();
	auto &g = sink_state->Cast<DeleteGlobalState>();
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(g.deleted_count));
		return SourceResultType::FINISHED;
	}

	g.return_collection.Scan(state.scan_state, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
