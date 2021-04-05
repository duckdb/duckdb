#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/string_segment.hpp"
#include "duckdb/storage/table/validity_segment.hpp"

namespace duckdb {

PersistentSegment::PersistentSegment(DatabaseInstance &db, block_id_t id, idx_t offset, const LogicalType &type_p,
                                     idx_t start, idx_t count, unique_ptr<BaseStatistics> statistics)
    : ColumnSegment(type_p, ColumnSegmentType::PERSISTENT, start, count, move(statistics)), db(db), block_id(id),
      offset(offset) {
	D_ASSERT(offset == 0);
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data = make_unique<StringSegment>(db, start, id);
		data->max_vector_count = count / STANDARD_VECTOR_SIZE + (count % STANDARD_VECTOR_SIZE == 0 ? 0 : 1);
	} else if (type.InternalType() == PhysicalType::BIT) {
		data = make_unique<ValiditySegment>(db, start, id);
	} else {
		data = make_unique<NumericSegment>(db, type.InternalType(), start, id);
	}
	data->tuple_count = count;
}

void PersistentSegment::InitializeScan(ColumnScanState &state) {
	data->InitializeScan(state);
}

void PersistentSegment::Scan(ColumnScanState &state, idx_t vector_index, Vector &result) {
	data->Scan(state, vector_index, result);
}

void PersistentSegment::Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) {
	data->Fetch(state, vector_index, result);
}

void PersistentSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	data->FetchRow(state, row_id - this->start, result, result_idx);
}

} // namespace duckdb
