#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

ValidityColumnData::ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                       ColumnDataType data_type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, LogicalType(LogicalTypeId::VALIDITY), data_type, parent) {
}

ValidityColumnData::ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                       ColumnData &parent)
    : ValidityColumnData(block_manager, info, column_index, parent.GetDataType(), parent) {
}

template <class SCAN>
static void ScanValidityMask(ColumnSegment &segment, Vector &result, idx_t count, ScanVectorType scan_type,
                             SCAN &&scan) {
	if (count == 0) {
		return;
	}
	if (scan_type == ScanVectorType::SCAN_ENTIRE_VECTOR) {
		if (segment.GetCompressionFunction().type == CompressionType::COMPRESSION_CONSTANT) {
			if (!segment.GetStats().CanHaveNull()) {
				return;
			}
			if (result.GetType().InternalType() == PhysicalType::STRUCT ||
			    result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
				ConstantVector::SetNull(result, count_t(count));
				return;
			}
		}
		result.Flatten();
	}
	scan(FlatVector::ValidityMutable(result));
}

void ValidityColumnData::ScanValidity(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                      Vector &result) {
	ScanValidityMask(segment, result, scan_count, ScanVectorType::SCAN_ENTIRE_VECTOR, [&](ValidityMask &mask) {
		segment.GetCompressionFunction().scan_validity(segment, state, scan_count, mask, 0,
		                                               ScanVectorType::SCAN_ENTIRE_VECTOR);
	});
}

void ValidityColumnData::ScanPartialValidity(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                             Vector &result, idx_t result_offset) {
	segment.GetCompressionFunction().scan_validity(segment, state, scan_count, FlatVector::ValidityMutable(result),
	                                               result_offset, ScanVectorType::SCAN_FLAT_VECTOR);
}

void ValidityColumnData::SelectValidity(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count,
                                        Vector &result, const SelectionVector &sel, idx_t sel_count,
                                        idx_t result_offset, ScanVectorType scan_type) {
	ScanValidityMask(segment, result, sel_count, scan_type, [&](ValidityMask &mask) {
		segment.GetCompressionFunction().select_validity(segment, state, vector_count, mask, sel, sel_count,
		                                                 result_offset);
	});
}

void ValidityColumnData::UpdateWithBase(TransactionData transaction, DuckTableEntry &table_entry, idx_t column_index,
                                        Vector &update_vector, row_t *row_ids, idx_t update_count, ColumnData &base,
                                        idx_t row_group_start) {
	D_ASSERT(update_count > 0);
	if (row_ids[0] < UnsafeNumericCast<row_t>(row_group_start)) {
		throw InternalException("ValidityColumnData::UpdateWithBase - row id out of range");
	}
	const idx_t row_offset = UnsafeNumericCast<idx_t>(row_ids[0]) - row_group_start;
	Vector base_vector(base.type, update_count);
	auto &segment = data.GetSegment(row_offset)->GetNode();
	if (segment.GetCompressionFunction().type == CompressionType::COMPRESSION_EMPTY) {
		// The validity is actually covered by the data, so we read it to get the validity for UpdateInternal.
		base.FetchBaseRows(row_ids, update_count, base_vector, row_group_start);
	} else {
		FetchBaseRows(row_ids, update_count, base_vector, row_group_start);
	}

	UpdateInternal(transaction, table_entry, column_index, update_vector, row_ids, update_count, base_vector,
	               row_group_start);
}

void ValidityColumnData::AppendData(ColumnAppendState &state, UnifiedVectorFormat &vdata, idx_t count) {
	ColumnData::AppendData(state, vdata, count);
}

struct ValidityColumnCheckpointState : public ColumnCheckpointState {
	ValidityColumnCheckpointState(const RowGroup &row_group, ColumnData &column_data, Context &context,
	                              PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, context, partial_block_manager) {
	}

public:
	shared_ptr<ColumnData> CreateEmptyColumnData() override {
		return make_shared_ptr<ValidityColumnData>(original_column.GetBlockManager(), original_column.GetTableInfo(),
		                                           original_column.column_index, ColumnDataType::CHECKPOINT_TARGET,
		                                           nullptr);
	}
};

unique_ptr<ColumnCheckpointState>
ValidityColumnData::CreateCheckpointState(const RowGroup &row_group, Context &context,
                                          PartialBlockManager &partial_block_manager) {
	return make_uniq<ValidityColumnCheckpointState>(row_group, *this, context, partial_block_manager);
}

void ValidityColumnData::Verify(RowGroup &parent) {
	D_ASSERT(HasParent());
	ColumnData::Verify(parent);
}

} // namespace duckdb
