#include "storage/table/persistent_segment.hpp"
#include "common/exception.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/types/null_value.hpp"
#include "storage/checkpoint/table_data_writer.hpp"
#include "storage/meta_block_reader.hpp"

#include "storage/numeric_segment.hpp"
#include "storage/string_segment.hpp"

using namespace duckdb;
using namespace std;

PersistentSegment::PersistentSegment(BufferManager &manager, block_id_t id, index_t offset, TypeId type, index_t start,
                                     index_t count)
    : ColumnSegment(type, ColumnSegmentType::PERSISTENT, start, count), manager(manager), block_id(id), offset(offset) {
	assert(offset == 0);
	if (type == TypeId::VARCHAR) {
		throw Exception("FIXME: not supported yet!");
	} else {
		data = make_unique<NumericSegment>(manager, type, id);
	}
	data->tuple_count = count;
	// FIXME
	stats.has_null = true;
}

void PersistentSegment::InitializeScan(ColumnScanState &state) {
	data->InitializeScan(state);
}

void PersistentSegment::Scan(Transaction &transaction, ColumnScanState &state, index_t vector_index, Vector &result) {
	data->Scan(transaction, state, vector_index, result);
}

void PersistentSegment::IndexScan(ColumnScanState &state, Vector &result) {
	data->IndexScan(state, state.vector_index, result);
}

void PersistentSegment::Fetch(ColumnScanState &state, index_t vector_index, Vector &result) {
	throw Exception("FIXME: not implemented");
}

void PersistentSegment::FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result) {
	throw Exception("FIXME: not implemented");
}

void PersistentSegment::Update(DataTable &table, Transaction &transaction, Vector &updates, row_t *ids) {
	throw Exception("FIXME: not implemented");
}