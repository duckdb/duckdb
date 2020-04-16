#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/string_segment.hpp"

using namespace duckdb;
using namespace std;

PersistentSegment::PersistentSegment(BufferManager &manager, block_id_t id, idx_t offset, TypeId type, idx_t start,
                                     idx_t count, data_t stats_min[], data_t stats_max[])
    : ColumnSegment(type, ColumnSegmentType::PERSISTENT, start, count, stats_min, stats_max), manager(manager),
      block_id(id), offset(offset) {
	assert(offset == 0);
	if (type == TypeId::VARCHAR) {
		data = make_unique<StringSegment>(manager, start, id);
		data->max_vector_count = count / STANDARD_VECTOR_SIZE + (count % STANDARD_VECTOR_SIZE == 0 ? 0 : 1);
	} else {
		data = make_unique<NumericSegment>(manager, type, start, id);
	}
	data->tuple_count = count;
}

void PersistentSegment::InitializeScan(ColumnScanState &state) {
	data->InitializeScan(state);
}

void PersistentSegment::Scan(Transaction &transaction, ColumnScanState &state, idx_t vector_index, Vector &result) {
	data->Scan(transaction, state, vector_index, result);
}

void PersistentSegment::FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result,
                                   SelectionVector &sel, idx_t &approved_tuple_count) {
	data->FilterScan(transaction, state, result, sel, approved_tuple_count);
}

void PersistentSegment::IndexScan(ColumnScanState &state, Vector &result) {
	data->IndexScan(state, state.vector_index, result);
}

void PersistentSegment::Select(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
                               idx_t &approved_tuple_count, vector<TableFilter> &tableFilter) {
	data->Select(transaction, result, tableFilter, sel, approved_tuple_count, state);
}

void PersistentSegment::Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) {
	data->Fetch(state, vector_index, result);
}

void PersistentSegment::FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
                                 idx_t result_idx) {
	data->FetchRow(state, transaction, row_id - this->start, result, result_idx);
}

void PersistentSegment::Update(ColumnData &column_data, Transaction &transaction, Vector &updates, row_t *ids,
                               idx_t count) {
	// update of persistent segment: check if the table has been updated before
	if (block_id == data->block_id) {
		// data has not been updated before! convert the segment from one that refers to an on-disk block to one that
		// refers to a in-memory buffer
		data->ToTemporary();
	}
	data->Update(column_data, stats, transaction, updates, ids, count, this->start);
}
