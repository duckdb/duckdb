#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/segment/compressed_segment.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

TransientSegment::TransientSegment(DatabaseInstance &db, const LogicalType &type_p, idx_t start)
    : ColumnSegment(db, type_p, ColumnSegmentType::TRANSIENT, start) {
	auto &config = DBConfig::GetConfig(db);
	data = make_unique<CompressedSegment>(db, type.InternalType(), start, config.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, type.InternalType()));
}

void TransientSegment::InitializeAppend(ColumnAppendState &state) {
}

idx_t TransientSegment::Append(ColumnAppendState &state, VectorData &append_data, idx_t offset, idx_t count) {
	idx_t appended = data->Append(stats, append_data, offset, count);
	this->count += appended;
	return appended;
}

void TransientSegment::RevertAppend(idx_t start_row) {
	data->RevertAppend(start_row);
	this->count = start_row - this->start;
}

} // namespace duckdb
