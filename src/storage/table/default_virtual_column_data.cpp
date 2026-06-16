#include "duckdb/storage/table/default_virtual_column_data.hpp"

namespace duckdb {

DefaultVirtualColumnData::DefaultVirtualColumnData(BlockManager &block_manager, DataTableInfo &info, column_t column_id,
                                                   LogicalType type)
    : ColumnData(block_manager, info, column_id, std::move(type), ColumnDataType::MAIN_TABLE, nullptr) {
	// synthetic columns are produced on the fly and are never NULL at the source (a NULL-padding join
	// is what introduces NULLs for some of them, e.g. row_is_present)
	stats->statistics.SetHasNoNullFast();
}

void DefaultVirtualColumnData::InitializePrefetch(PrefetchState &, ColumnScanState &, idx_t) {
	throw InternalException(GetColumnDataName() + " does not support InitializePrefetch");
}

void DefaultVirtualColumnData::InitializeScan(ColumnScanState &) {
	throw InternalException(GetColumnDataName() + " does not support InitializeScan");
}

void DefaultVirtualColumnData::InitializeScanWithOffset(ColumnScanState &, idx_t) {
	throw InternalException(GetColumnDataName() + " does not support InitializeScanWithOffset");
}

idx_t DefaultVirtualColumnData::Scan(TransactionData, idx_t, ColumnScanState &, Vector &, idx_t) {
	throw InternalException(GetColumnDataName() + " does not support Scan");
}

void DefaultVirtualColumnData::ScanCommittedRange(idx_t, idx_t, idx_t, Vector &) {
	throw InternalException(GetColumnDataName() + " does not support ScanCommittedRange");
}

idx_t DefaultVirtualColumnData::ScanCount(ColumnScanState &, Vector &, idx_t, idx_t) {
	throw InternalException(GetColumnDataName() + " does not support ScanCount");
}

void DefaultVirtualColumnData::Filter(TransactionData, idx_t, ColumnScanState &, Vector &, SelectionVector &, idx_t &,
                                      const TableFilter &, TableFilterState &) {
	throw InternalException(GetColumnDataName() + " does not support Filter");
}

void DefaultVirtualColumnData::Select(TransactionData, idx_t, ColumnScanState &, Vector &, SelectionVector &, idx_t) {
	throw InternalException(GetColumnDataName() + " does not support Select");
}

idx_t DefaultVirtualColumnData::Fetch(ColumnScanState &, row_t, Vector &) {
	throw InternalException(GetColumnDataName() + " does not support Fetch");
}

void DefaultVirtualColumnData::FetchRows(TransactionData, ColumnFetchState &, const StorageIndex &, const idx_t *,
                                         const SelectionVector &, idx_t, Vector &, idx_t) {
	throw InternalException(GetColumnDataName() + " does not support FetchRows");
}

void DefaultVirtualColumnData::Skip(ColumnScanState &, idx_t) {
	throw InternalException(GetColumnDataName() + " does not support Skip");
}

FilterPropagateResult DefaultVirtualColumnData::CheckZonemap(ColumnScanState &, TableFilter &) {
	throw InternalException(GetColumnDataName() + " does not support CheckZonemap");
}

void DefaultVirtualColumnData::InitializeAppend(ColumnAppendState &) {
	throw InternalException(GetColumnDataName() + " cannot be appended to");
}

void DefaultVirtualColumnData::Append(ColumnAppendState &, const Vector &, idx_t) {
	throw InternalException(GetColumnDataName() + " cannot be appended to");
}

void DefaultVirtualColumnData::AppendData(ColumnAppendState &, UnifiedVectorFormat &, idx_t) {
	throw InternalException(GetColumnDataName() + " cannot be appended to");
}

void DefaultVirtualColumnData::RevertAppend(row_t) {
	throw InternalException(GetColumnDataName() + " cannot be appended to");
}

void DefaultVirtualColumnData::Update(TransactionData, DuckTableEntry &, idx_t, Vector &, row_t *, idx_t, idx_t) {
	throw InternalException(GetColumnDataName() + " cannot be updated");
}

void DefaultVirtualColumnData::UpdateColumn(TransactionData, DuckTableEntry &, const vector<column_t> &, Vector &,
                                            row_t *, idx_t, idx_t, idx_t) {
	throw InternalException(GetColumnDataName() + " cannot be updated");
}

void DefaultVirtualColumnData::VisitBlockIds(BlockIdVisitor &) const {
	throw InternalException(GetColumnDataName() + " does not support VisitBlockIds");
}

unique_ptr<ColumnCheckpointState> DefaultVirtualColumnData::CreateCheckpointState(const RowGroup &,
                                                                                  PartialBlockManager &) {
	throw InternalException(GetColumnDataName() + " cannot be checkpointed");
}

unique_ptr<ColumnCheckpointState> DefaultVirtualColumnData::Checkpoint(const RowGroup &, ColumnCheckpointInfo &,
                                                                       const BaseStatistics &) {
	throw InternalException(GetColumnDataName() + " cannot be checkpointed");
}

void DefaultVirtualColumnData::CheckpointScan(ColumnSegment &, ColumnScanState &, idx_t, Vector &) const {
	throw InternalException(GetColumnDataName() + " cannot be checkpointed");
}

bool DefaultVirtualColumnData::IsPersistent() {
	throw InternalException(GetColumnDataName() + " cannot be persisted");
}

} // namespace duckdb
