#include "duckdb/storage/table/geo_column_data.hpp"

#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// GeoColumnData
//----------------------------------------------------------------------------------------------------------------------
// TODO: Override ::Filter to push down bounding box filters.

GeoColumnData::GeoColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, LogicalType type,
                             ColumnDataType data_type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, std::move(type), data_type, parent) {
	if (data_type != ColumnDataType::CHECKPOINT_TARGET) {
		base_column =
		    make_shared_ptr<StandardColumnData>(block_manager, info, column_index, std::move(type), data_type, this);
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Scan
//----------------------------------------------------------------------------------------------------------------------

void GeoColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) {
	return base_column->InitializePrefetch(prefetch_state, scan_state, rows);
}

void GeoColumnData::InitializeChildScanStates(ColumnScanState &state) {
	// Reset, inner layout might be different
	state.child_states.clear();

	// Validity
	state.child_states.emplace_back(state.parent);
	state.child_states[0].scan_options = state.scan_options;

	if (base_column->type.id() == LogicalTypeId::GEOMETRY) {
		// No need to reshape the child scan states
		return;
	}

	// Initialize point XY sub columns

	// X
	state.child_states.emplace_back(state.parent);
	state.child_states[1].Initialize(state.context, LogicalTypeId::DOUBLE, state.scan_options);

	// Y
	state.child_states.emplace_back(state.parent);
	state.child_states[2].Initialize(state.context, LogicalTypeId::DOUBLE, state.scan_options);

	// Scan both child columns
	state.scan_child_column.resize(2, true);
}

void GeoColumnData::InitializeScan(ColumnScanState &state) {
	InitializeChildScanStates(state);
	return base_column->InitializeScan(state);
}

void GeoColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	InitializeChildScanStates(state);
	return base_column->InitializeScanWithOffset(state, row_idx);
}

idx_t GeoColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                          idx_t target_count) {
	auto &layout_type = base_column->GetType();

	// Not a shredded column, so just emit the binary format immediately
	if (layout_type.id() == LogicalTypeId::GEOMETRY) {
		return base_column->Scan(transaction, vector_index, state, result, target_count);
	}

	// Setup an intermediate chunk to scan the actual data, based on how much we actually scanned
	// TODO: Put this in a scan state?
	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), {layout_type}, target_count);

	const auto scan_count = base_column->Scan(transaction, vector_index, state, scan_chunk.data[0], target_count);

	// Now reassemble
	Reassemble(scan_chunk.data[0], result, scan_count);
	return scan_count;
}

idx_t GeoColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
                                   idx_t target_count) {
	auto &layout_type = base_column->GetType();

	// Not a shredded column, so just emit the binary format immediately
	if (layout_type.id() == LogicalTypeId::GEOMETRY) {
		return base_column->ScanCommitted(vector_index, state, result, target_count);
	}

	// Setup an intermediate chunk to scan the actual data, based on how much we actually scanned
	// TODO: Put this in a scan state
	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), {layout_type}, target_count);

	const auto scan_count = base_column->ScanCommitted(vector_index, state, scan_chunk.data[0], target_count);

	// Now reassemble
	Reassemble(scan_chunk.data[0], result, scan_count);
	return scan_count;
}

idx_t GeoColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset) {
	// TODO: Is this enough?
	return base_column->ScanCount(state, result, count, result_offset);
}

void GeoColumnData::Skip(ColumnScanState &state, idx_t count) {
	return base_column->Skip(state);
}

//----------------------------------------------------------------------------------------------------------------------
// Append
//----------------------------------------------------------------------------------------------------------------------

void GeoColumnData::InitializeAppend(ColumnAppendState &state) {
	base_column->InitializeAppend(state);
}
void GeoColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t add_count) {
	base_column->Append(stats, state, vector, add_count);
	count += add_count;
}
void GeoColumnData::RevertAppend(row_t new_count) {
	base_column->RevertAppend(new_count);
	count = UnsafeNumericCast<idx_t>(new_count);
}

//----------------------------------------------------------------------------------------------------------------------
// Fetch
//----------------------------------------------------------------------------------------------------------------------

idx_t GeoColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	return base_column->Fetch(state, row_id, result);
}
void GeoColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                             idx_t result_idx) {
	return base_column->FetchRow(transaction, state, row_id, result, result_idx);
}

//----------------------------------------------------------------------------------------------------------------------
// Update
//----------------------------------------------------------------------------------------------------------------------

void GeoColumnData::Update(TransactionData transaction, DataTable &data_table, idx_t column_index,
                           Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t row_group_start) {
	return base_column->Update(transaction, data_table, column_index, update_vector, row_ids, update_count,
	                           row_group_start);
}
void GeoColumnData::UpdateColumn(TransactionData transaction, DataTable &data_table,
                                 const vector<column_t> &column_path, Vector &update_vector, row_t *row_ids,
                                 idx_t update_count, idx_t depth, idx_t row_group_start) {
	return base_column->UpdateColumn(transaction, data_table, column_path, update_vector, row_ids, update_count, depth,
	                                 row_group_start);
}

unique_ptr<BaseStatistics> GeoColumnData::GetUpdateStatistics() {
	return base_column->GetUpdateStatistics();
}

//----------------------------------------------------------------------------------------------------------------------
// Checkpoint
//----------------------------------------------------------------------------------------------------------------------
namespace {

class GeoColumnCheckpointState final : public ColumnCheckpointState {
public:
	GeoColumnCheckpointState(const RowGroup &row_group, ColumnData &column_data,
	                         PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		// Make stats
		global_stats = GeometryStats::CreateEmpty(column_data.type).ToUnique();
	}

	// Shared pointer to the new/old inner column.
	// This is never actually used here, but needs to stay alive
	// for as long as the checkpoint state, hence the shared_ptr.
	shared_ptr<ColumnData> inner_column;

	// The checkpoint state for the inner column.
	unique_ptr<ColumnCheckpointState> inner_column_state;

	shared_ptr<ColumnData> CreateEmptyColumnData() override {
		auto new_column = make_shared_ptr<GeoColumnData>(
		    original_column.GetBlockManager(), original_column.GetTableInfo(), original_column.column_index,
		    original_column.type, ColumnDataType::CHECKPOINT_TARGET, nullptr);
		return std::move(new_column);
	}

	shared_ptr<ColumnData> GetFinalResult() override {
		if (!result_column) {
			result_column = CreateEmptyColumnData();
		}

		auto &column_data = result_column->Cast<GeoColumnData>();

		auto new_inner = inner_column_state->GetFinalResult();
		new_inner->SetParent(column_data);
		column_data.base_column = std::move(new_inner);
		;

		return ColumnCheckpointState::GetFinalResult();
	}

	unique_ptr<BaseStatistics> GetStatistics() override {
		D_ASSERT(global_stats);

		// Merge stats!
		// Actually, we might not need to do this here, but do it
		// when we scan/append into then we split data instead.

		return std::move(global_stats);
	}

	PersistentColumnData ToPersistentData() override {
		return inner_column_state->ToPersistentData();
	}
};

} // namespace

unique_ptr<ColumnCheckpointState> GeoColumnData::CreateCheckpointState(const RowGroup &row_group,
                                                                       PartialBlockManager &partial_block_manager) {
	// return base_column->CreateCheckpointState(row_group, partial_block_manager);
	throw NotImplementedException("GeoColumnData CreateCheckpointState is not implemented yet.");
}

unique_ptr<ColumnCheckpointState> GeoColumnData::Checkpoint(const RowGroup &row_group, ColumnCheckpointInfo &info) {
	auto &partial_block_manager = info.GetPartialBlockManager();
	auto checkpoint_state = make_uniq<GeoColumnCheckpointState>(row_group, *this, partial_block_manager);

	if (!HasAnyChanges()) {
		// No changes, keep column
		checkpoint_state->inner_column_state = base_column->Checkpoint(row_group, info);
		return std::move(checkpoint_state);
	}

	// Figure out if this segment can use an alternative type layout
	auto layout_type = GetLayoutType();

	if (layout_type.id() == LogicalTypeId::INVALID) {
		// Cant specialize, keep column
		checkpoint_state->inner_column_state = base_column->Checkpoint(row_group, info);
		return std::move(checkpoint_state);
	}

	auto new_column =
	    CreateColumn(block_manager, this->info, base_column->column_index, layout_type, GetDataType(), this);

	// Setup scan from the old column
	DataChunk scan_chunk;
	ColumnScanState scan_state(nullptr);
	scan_chunk.Initialize(Allocator::DefaultAllocator(), {base_column->type}, STANDARD_VECTOR_SIZE);
	InitializeScan(scan_state);

	// Setup append to the new column
	DataChunk append_chunk;
	ColumnAppendState append_state;
	append_chunk.Initialize(Allocator::DefaultAllocator(), {new_column->type}, STANDARD_VECTOR_SIZE);
	new_column->InitializeAppend(append_state);

	idx_t total_count = count.load();
	idx_t vector_index = 0;

	for (idx_t scanned = 0; scanned < total_count; scanned += STANDARD_VECTOR_SIZE) {
		scan_chunk.Reset();

		auto to_scan = MinValue(total_count - scanned, static_cast<idx_t>(STANDARD_VECTOR_SIZE));
		ScanCommitted(vector_index++, scan_state, scan_chunk.data[0], false, to_scan);

		// Verify the scan chunk
		scan_chunk.Verify();

		append_chunk.Reset();
		append_chunk.SetCardinality(to_scan);

		// Make the split
		Specialize(scan_chunk.data[0], append_chunk.data[0], to_scan);

		// Append into the new specialized column
		// TODO: Keep this stats around, and merge into the actual stats
		auto dummy_stats = BaseStatistics::CreateEmpty(new_column->GetType());
		new_column->Append(dummy_stats, append_state, append_chunk.data[0], to_scan);

		InterpretStats(dummy_stats, *checkpoint_state->global_stats);
	}

	// Move then new column into our checkpoint state
	checkpoint_state->inner_column = std::move(new_column);
	checkpoint_state->inner_column_state = checkpoint_state->inner_column->Checkpoint(row_group, info);

	return std::move(checkpoint_state);
}

bool GeoColumnData::IsPersistent() {
	return base_column->IsPersistent();
}

bool GeoColumnData::HasAnyChanges() const {
	return base_column->HasAnyChanges();
}

PersistentColumnData GeoColumnData::Serialize() {
	// TODO: might want to write shredding state...
	return base_column->Serialize();
}

void GeoColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	base_column->InitializeColumn(column_data, target_stats);
	count = base_column->count.load();
}

//----------------------------------------------------------------------------------------------------------------------
// Misc
//----------------------------------------------------------------------------------------------------------------------

idx_t GeoColumnData::GetMaxEntry() {
	return base_column->GetMaxEntry();
}

void GeoColumnData::GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index, vector<idx_t> col_path,
                                         vector<ColumnSegmentInfo> &result) {
	return base_column->GetColumnSegmentInfo(context, row_group_index, col_path, result);
}

void GeoColumnData::Verify(RowGroup &parent) {
	return base_column->Verify(parent);
}

//----------------------------------------------------------------------------------------------------------------------
// Specialize
//----------------------------------------------------------------------------------------------------------------------
LogicalType GeoColumnData::GetLayoutType() const {
	// Get the stats of this column
	auto &stats = this->stats->statistics;
	const auto &types = GeometryStats::GetTypes(stats);

	if (types.HasOnly(GeometryType::POINT, VertexType::XY)) {
		// Push POINT_XY type
		return LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}});
	}

	/*
	if (types.HasOnly(GeometryType::POLYGON, VertexType::XY)) {
	    // Push POLYGON_XY type
	    return LogicalType::LIST(
	        LogicalType::LIST(LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}})));
	}
	*/

	return LogicalTypeId::INVALID;
}

static void ShredPoints(Vector &source_vec, Vector &target_vec, idx_t count) {
	auto &parts = StructVector::GetEntries(target_vec);
	auto &x_vec = *parts[0];
	auto &y_vec = *parts[1];
	const auto x_data = FlatVector::GetData<double>(x_vec);
	const auto y_data = FlatVector::GetData<double>(y_vec);

	// TODO: This is cheating and should be generalized:
	UnifiedVectorFormat scan_uvu;
	source_vec.ToUnifiedFormat(count, scan_uvu);
	const auto scan_data = UnifiedVectorFormat::GetData<string_t>(scan_uvu);

	for (idx_t res_idx = 0; res_idx < count; res_idx++) {
		const auto row_idx = scan_uvu.sel->get_index(res_idx);

		if (!scan_uvu.validity.RowIsValid(row_idx)) {
			FlatVector::SetNull(target_vec, res_idx, true);
			continue;
		}

		const auto &blob = scan_data[row_idx];
#ifdef DEBUG
		const auto type = Geometry::GetType(blob);
		D_ASSERT(type.first == GeometryType::POINT && type.second == VertexType::XY);
#endif

		// Shred!
		const auto data = blob.GetData();
		// X/Y is at (1 + 4) offset
		memcpy(&x_data[res_idx], data + sizeof(uint8_t) + sizeof(uint32_t), sizeof(double));
		memcpy(&y_data[res_idx], data + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(double), sizeof(double));
	}
}

void GeoColumnData::Specialize(Vector &source, Vector &target, idx_t count) {
	// TODO: Check for other layouts
	ShredPoints(source, target, count);
}

static void UnshredPoints(Vector &geom_vec, Vector &result, idx_t count) {
	UnifiedVectorFormat geom_uvu;

	geom_vec.ToUnifiedFormat(count, geom_uvu);

	const auto &parts = StructVector::GetEntries(geom_vec);
	const auto &x_vec = *parts[0];
	const auto &y_vec = *parts[1];

	const auto result_data = FlatVector::GetData<string_t>(result);
	const auto x_data = FlatVector::GetData<double>(x_vec);
	const auto y_data = FlatVector::GetData<double>(y_vec);

	for (idx_t res_idx = 0; res_idx < count; res_idx++) {
		const auto geom_idx = geom_uvu.sel->get_index(res_idx);
		const auto geom_valid = geom_uvu.validity.RowIsValid(geom_idx);

		// Both null
		if (!geom_valid) {
			FlatVector::SetNull(result, res_idx, true);
			continue;
		}

		char buffer[1 + 4 + 8 + 8];
		memcpy(buffer, "\x01\x01\x00\x00\x00", 5); // POINT type
		memcpy(buffer + 5, &x_data[geom_idx], 8);
		memcpy(buffer + 13, &y_data[geom_idx], 8);
		result_data[res_idx] = StringVector::AddStringOrBlob(result, string_t(buffer, sizeof(buffer)));
	}
}

void GeoColumnData::Reassemble(Vector &source, Vector &target, idx_t count) {
	UnshredPoints(source, target, count);
}

void GeoColumnData::InterpretStats(BaseStatistics &source, BaseStatistics &target) {
	// TODO: We need to track what we shredded to

	// Copy base stats
	// TODO: Think about DISTINCT stats and how they affect shredding
	target.CopyBase(source);

	// Set extent
	const auto struct_stats = StructStats::GetChildStats(source);

	auto &extent = GeometryStats::GetExtent(target);
	extent.x_min = NumericStats::GetMin<double>(struct_stats[0]);
	extent.x_max = NumericStats::GetMax<double>(struct_stats[0]);
	extent.y_min = NumericStats::GetMin<double>(struct_stats[1]);
	extent.y_max = NumericStats::GetMax<double>(struct_stats[1]);

	// Set types
	auto &types = GeometryStats::GetTypes(target);
	types.Clear();
	types.Add(GeometryType::POINT, VertexType::XY);
}

} // namespace duckdb
