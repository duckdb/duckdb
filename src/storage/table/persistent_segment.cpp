#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include "duckdb/storage/segment/constant_segment.hpp"
#include "duckdb/storage/segment/numeric_segment.hpp"
#include "duckdb/storage/segment/string_segment.hpp"
#include "duckdb/storage/table/validity_segment.hpp"

namespace duckdb {

PersistentSegment::PersistentSegment(DatabaseInstance &db, block_id_t id, idx_t offset, const LogicalType &type_p,
                                     idx_t start, idx_t count, unique_ptr<BaseStatistics> statistics)
    : ColumnSegment(db, type_p, ColumnSegmentType::PERSISTENT, start, count, move(statistics)), block_id(id),
      offset(offset) {
	D_ASSERT(offset == 0);
	if (block_id == INVALID_BLOCK) {
		data = make_unique<ConstantSegment>(db, stats, start);
	} else if (type.InternalType() == PhysicalType::VARCHAR) {
		data = make_unique<StringSegment>(db, start, id);
	} else if (type.InternalType() == PhysicalType::BIT) {
		data = make_unique<ValiditySegment>(db, start, id);
	} else {
		data = make_unique<NumericSegment>(db, type.InternalType(), start, id);
	}
	data->tuple_count = count;
}

} // namespace duckdb
