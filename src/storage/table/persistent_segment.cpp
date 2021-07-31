#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include "duckdb/storage/segment/compressed_segment.hpp"

namespace duckdb {

PersistentSegment::PersistentSegment(DatabaseInstance &db, block_id_t id, idx_t offset, const LogicalType &type_p,
                                     idx_t start, idx_t count, unique_ptr<BaseStatistics> statistics)
    : ColumnSegment(db, type_p, ColumnSegmentType::PERSISTENT, start, count, move(statistics)), block_id(id),
      offset(offset) {
	D_ASSERT(offset == 0);
	auto &config = DBConfig::GetConfig(db);
	if (block_id == INVALID_BLOCK) {
		data = make_unique<CompressedSegment>(this, db, type.InternalType(), start, config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, type.InternalType()), id);
	} else {
		auto &config = DBConfig::GetConfig(db);
		data = make_unique<CompressedSegment>(this, db, type.InternalType(), start, config.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, type.InternalType()), id);
	}
	data->tuple_count = count;
}

} // namespace duckdb
