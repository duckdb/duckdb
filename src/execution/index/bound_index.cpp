#include "duckdb/execution/index/bound_index.hpp"

#include "duckdb/common/radix.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include <array>

namespace duckdb {

//-------------------------------------------------------------------------------
// Bound index
//-------------------------------------------------------------------------------

BoundIndex::BoundIndex(const string &name, const string &index_type, IndexConstraintType index_constraint_type,
                       const vector<column_t> &column_ids, TableIOManager &table_io_manager,
                       const vector<unique_ptr<Expression>> &unbound_expressions_p, AttachedDatabase &db)
    : Index(column_ids, table_io_manager, db), name(name), index_type(index_type),
      index_constraint_type(index_constraint_type) {

	for (auto &expr : unbound_expressions_p) {
		types.push_back(expr->return_type.InternalType());
		logical_types.push_back(expr->return_type);
		unbound_expressions.emplace_back(expr->Copy());
		bound_expressions.push_back(BindExpression(expr->Copy()));
		executor.AddExpression(*bound_expressions.back());
	}
}

void BoundIndex::InitializeLock(IndexLock &state) {
	state.index_lock = unique_lock<mutex>(lock);
}

ErrorData BoundIndex::Append(DataChunk &chunk, Vector &row_ids) {
	IndexLock l;
	InitializeLock(l);
	return Append(l, chunk, row_ids);
}

ErrorData BoundIndex::Append(IndexLock &l, DataChunk &chunk, Vector &row_ids, IndexAppendInfo &info) {
	// Fallback to the old Append.
	return Append(l, chunk, row_ids);
}

ErrorData BoundIndex::Append(DataChunk &chunk, Vector &row_ids, IndexAppendInfo &info) {
	IndexLock l;
	InitializeLock(l);
	return Append(l, chunk, row_ids, info);
}

void BoundIndex::VerifyAppend(DataChunk &chunk, IndexAppendInfo &info, optional_ptr<ConflictManager> manager) {
	throw NotImplementedException("this implementation of VerifyAppend does not exist.");
}

void BoundIndex::VerifyConstraint(DataChunk &chunk, IndexAppendInfo &info, ConflictManager &manager) {
	throw NotImplementedException("this implementation of VerifyConstraint does not exist.");
}

void BoundIndex::CommitDrop() {
	IndexLock index_lock;
	InitializeLock(index_lock);
	CommitDrop(index_lock);
}

void BoundIndex::Delete(DataChunk &entries, Vector &row_identifiers) {
	IndexLock state;
	InitializeLock(state);
	Delete(state, entries, row_identifiers);
}

ErrorData BoundIndex::Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids, IndexAppendInfo &info) {
	throw NotImplementedException("this implementation of Insert does not exist.");
}

bool BoundIndex::MergeIndexes(BoundIndex &other_index) {
	IndexLock state;
	InitializeLock(state);
	return MergeIndexes(state, other_index);
}

string BoundIndex::VerifyAndToString(const bool only_verify) {
	IndexLock l;
	InitializeLock(l);
	return VerifyAndToString(l, only_verify);
}

void BoundIndex::VerifyAllocations() {
	IndexLock l;
	InitializeLock(l);
	return VerifyAllocations(l);
}

void BoundIndex::VerifyBuffers(IndexLock &l) {
	throw NotImplementedException("this implementation of VerifyBuffers does not exist");
}

void BoundIndex::VerifyBuffers() {
	IndexLock l;
	InitializeLock(l);
	return VerifyBuffers(l);
}

void BoundIndex::Vacuum() {
	IndexLock state;
	InitializeLock(state);
	Vacuum(state);
}

idx_t BoundIndex::GetInMemorySize() {
	IndexLock state;
	InitializeLock(state);
	return GetInMemorySize(state);
}

void BoundIndex::ExecuteExpressions(DataChunk &input, DataChunk &result) {
	executor.Execute(input, result);
}

unique_ptr<Expression> BoundIndex::BindExpression(unique_ptr<Expression> root_expr) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    root_expr, [&](BoundColumnRefExpression &bound_colref, unique_ptr<Expression> &expr) {
		    expr =
		        make_uniq<BoundReferenceExpression>(expr->return_type, column_ids[bound_colref.binding.column_index]);
	    });
	return root_expr;
}

bool BoundIndex::IndexIsUpdated(const vector<PhysicalIndex> &column_ids_p) const {
	for (auto &column : column_ids_p) {
		if (column_id_set.find(column.index) != column_id_set.end()) {
			return true;
		}
	}
	return false;
}

IndexStorageInfo BoundIndex::SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options) {
	throw NotImplementedException("The implementation of this index disk serialization does not exist.");
}

IndexStorageInfo BoundIndex::SerializeToWAL(const case_insensitive_map_t<Value> &options) {
	throw NotImplementedException("The implementation of this index WAL serialization does not exist.");
}

string BoundIndex::AppendRowError(DataChunk &input, idx_t index) {
	string error;
	for (idx_t c = 0; c < input.ColumnCount(); c++) {
		if (c > 0) {
			error += ", ";
		}
		error += input.GetValue(c, index).ToString();
	}
	return error;
}

namespace {

struct BufferedReplayState {
	unique_ptr<ColumnDataCollection> buffer = nullptr;
	ColumnDataScanState scan_state;
	DataChunk current_chunk;
	bool scan_initialized = false;
};
} // namespace

void BoundIndex::ApplyBufferedReplays(const vector<LogicalType> &table_types, BufferedIndexReplays &buffered_replays,
                                      const vector<StorageIndex> &mapped_column_ids) {
	if (!buffered_replays.HasBufferedReplays()) {
		return;
	}

	std::array<BufferedReplayState, 2> replay_states;
	DataChunk table_chunk;
	table_chunk.InitializeEmpty(table_types);

	for (auto &replay_range : buffered_replays.ranges) {
		auto type_idx = static_cast<size_t>(replay_range.type);
		auto &state = replay_states[type_idx];

		if (!state.scan_initialized) {
			state.buffer = std::move(buffered_replays.GetBuffer(replay_range.type));
			state.buffer->InitializeScan(state.scan_state);
			state.buffer->InitializeScanChunk(state.current_chunk);
			state.scan_initialized = true;
		}

		idx_t current_row = replay_range.start;
		while (current_row < replay_range.end) {
			if (current_row < state.scan_state.current_row_index || current_row >= state.scan_state.next_row_index) {
				if (!state.buffer->Scan(state.scan_state, state.current_chunk)) {
					throw InternalException("Buffered index data exhausted during replay");
				}
				continue;
			}

			auto offset_in_chunk = current_row - state.scan_state.current_row_index;
			auto available_in_chunk = state.current_chunk.size() - offset_in_chunk;
			auto rows_to_process = MinValue<idx_t>(available_in_chunk, replay_range.end - current_row);

			SelectionVector sel(offset_in_chunk, rows_to_process);

			table_chunk.Reset();
			for (idx_t col_idx = 0; col_idx < state.current_chunk.ColumnCount() - 1; col_idx++) {
				auto col_id = mapped_column_ids[col_idx].GetPrimaryIndex();
				table_chunk.data[col_id].Reference(state.current_chunk.data[col_idx]);
			}
			table_chunk.Slice(sel, rows_to_process);

			Vector row_ids(state.current_chunk.data.back(), sel, rows_to_process);

			// Apply the replay operation
			if (replay_range.type == BufferedIndexReplay::INSERT_ENTRY) {
				IndexAppendInfo append_info(IndexAppendMode::INSERT_DUPLICATES, nullptr);
				auto error = Append(table_chunk, row_ids, append_info);
				if (error.HasError()) {
					throw InternalException("error while applying buffered appends: " + error.Message());
				}
			} else {
				Delete(table_chunk, row_ids);
			}

			current_row += rows_to_process;
		}
	}
}

} // namespace duckdb
