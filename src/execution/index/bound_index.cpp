#include "duckdb/execution/index/bound_index.hpp"

#include "duckdb/common/radix.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/storage/table/append_state.hpp"

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

static void ReplayChunk(BoundIndex &index, BufferedIndexReplay type, DataChunk &scan_chunk,
                        const vector<LogicalType> &table_types, const vector<StorageIndex> &mapped_column_ids) {
	if (scan_chunk.size() == 0) {
		return;
	}
	DataChunk table_chunk;
	table_chunk.InitializeEmpty(table_types);

	for (idx_t i = 0; i < scan_chunk.ColumnCount() - 1; i++) {
		auto col_id = mapped_column_ids[i].GetPrimaryIndex();
		table_chunk.data[col_id].Reference(scan_chunk.data[i]);
	}
	table_chunk.SetCardinality(scan_chunk.size());

	switch (type) {
	case BufferedIndexReplay::INSERT_ENTRY: {
		IndexAppendInfo index_append_info(IndexAppendMode::INSERT_DUPLICATES, nullptr);
		auto error = index.Append(table_chunk, scan_chunk.data.back(), index_append_info);
		if (error.HasError()) {
			throw InternalException("error while applying buffered appends: " + error.Message());
		}
		return;
	}
	case BufferedIndexReplay::DEL_ENTRY: {
		index.Delete(table_chunk, scan_chunk.data.back());
	}
	}
}

} // namespace

void BoundIndex::ApplyBufferedReplays(const vector<LogicalType> &table_types,
                                      vector<BufferedIndexData> &buffered_replays,
                                      const vector<StorageIndex> &mapped_column_ids) {
	for (auto &replay : buffered_replays) {

		if (replay.data) {
			ColumnDataScanState state;
			auto &buffered_data = *replay.data;
			buffered_data.InitializeScan(state);

			DataChunk scan_chunk;
			buffered_data.InitializeScanChunk(scan_chunk);

			while (buffered_data.Scan(state, scan_chunk)) {
				ReplayChunk(*this, replay.type, scan_chunk, table_types, mapped_column_ids);
			}
		}
		if (replay.small_chunk && replay.small_chunk->size() > 0) {
			ReplayChunk(*this, replay.type, *replay.small_chunk, table_types, mapped_column_ids);
		}
	}
}

} // namespace duckdb
