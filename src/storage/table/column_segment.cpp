#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/segment/compressed_segment.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <cstring>

namespace duckdb {

unique_ptr<ColumnSegment> ColumnSegment::CreatePersistentSegment(DatabaseInstance &db, block_id_t block_id, idx_t offset, const LogicalType &type, idx_t start, idx_t count, unique_ptr<BaseStatistics> statistics) {
	D_ASSERT(offset == 0);
	unique_ptr<CompressedSegment> data;
	auto &config = DBConfig::GetConfig(db);
	if (block_id == INVALID_BLOCK) {
		data = make_unique<CompressedSegment>(nullptr, db, type.InternalType(), start, config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, type.InternalType()), block_id);
	} else {
		auto &config = DBConfig::GetConfig(db);
		data = make_unique<CompressedSegment>(nullptr, db, type.InternalType(), start, config.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, type.InternalType()), block_id);
	}
	data->tuple_count = count;
	return make_unique<ColumnSegment>(db, type, ColumnSegmentType::PERSISTENT, start, count, move(statistics), move(data), block_id, offset);
}

unique_ptr<ColumnSegment> ColumnSegment::CreateTransientSegment(DatabaseInstance &db, const LogicalType &type, idx_t start) {
	auto &config = DBConfig::GetConfig(db);
	auto data = make_unique<CompressedSegment>(nullptr, db, type.InternalType(), start, config.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, type.InternalType()));
	return make_unique<ColumnSegment>(db, type, ColumnSegmentType::TRANSIENT, start, 0, move(data));
}


ColumnSegment::ColumnSegment(DatabaseInstance &db, LogicalType type_p, ColumnSegmentType segment_type, idx_t start,
                             idx_t count, unique_ptr<CompressedSegment> data_p)
    : SegmentBase(start, count), db(db), type(move(type_p)), type_size(GetTypeIdSize(type.InternalType())),
      segment_type(segment_type), stats(type), data(move(data_p)), block_id(INVALID_BLOCK), offset(idx_t(-1)) {
	data->parent = this;
}

ColumnSegment::ColumnSegment(DatabaseInstance &db, LogicalType type_p, ColumnSegmentType segment_type, idx_t start,
                             idx_t count, unique_ptr<BaseStatistics> statistics, unique_ptr<CompressedSegment> data_p,
							 block_id_t block_id_p, idx_t offset_p)
    : SegmentBase(start, count), db(db), type(move(type_p)), type_size(GetTypeIdSize(type.InternalType())),
      segment_type(segment_type), stats(type, move(statistics)), data(move(data_p)), block_id(block_id_p), offset(offset_p) {
	data->parent = this;
}

ColumnSegment::~ColumnSegment() {
}

void ColumnSegment::InitializeScan(ColumnScanState &state) {
	data->InitializeScan(state);
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t start_row, idx_t scan_count, Vector &result, idx_t result_offset,
                         bool entire_vector) {
	D_ASSERT(start_row + scan_count <= this->count);
	if (entire_vector) {
		D_ASSERT(result_offset == 0);
		data->Scan(state, start_row, scan_count, result);
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		data->ScanPartial(state, start_row, scan_count, result, result_offset);
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	}
}

void ColumnSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	data->FetchRow(state, row_id - this->start, result, result_idx);
}

void ColumnSegment::InitializeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
}

idx_t ColumnSegment::Append(ColumnAppendState &state, VectorData &append_data, idx_t offset, idx_t count) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	idx_t appended = data->Append(stats, append_data, offset, count);
	this->count += appended;
	return appended;
}

void ColumnSegment::RevertAppend(idx_t start_row) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	data->RevertAppend(start_row);
	this->count = start_row - this->start;
}

} // namespace duckdb
