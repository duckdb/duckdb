#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/string_segment.hpp"
#include "duckdb/storage/table/validity_segment.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

TransientSegment::TransientSegment(DatabaseInstance &db, const LogicalType &type_p, idx_t start)
    : ColumnSegment(db, type_p, ColumnSegmentType::TRANSIENT, start) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data = make_unique<StringSegment>(db, start);
	} else if (type.InternalType() == PhysicalType::BIT) {
		data = make_unique<ValiditySegment>(db, start);
	} else {
		data = make_unique<NumericSegment>(db, type.InternalType(), start);
	}
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
