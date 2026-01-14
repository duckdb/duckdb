#include "duckdb/storage/table/geo_column_data.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"

#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// GeoColumnData
//----------------------------------------------------------------------------------------------------------------------
// TODO: Override ::Filter to push down bounding box filters.

GeoColumnData::GeoColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, LogicalType type_p,
                             ColumnDataType data_type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, std::move(type_p), data_type, parent) {
	if (data_type != ColumnDataType::CHECKPOINT_TARGET) {
		base_column = make_shared_ptr<StandardColumnData>(block_manager, info, column_index, type, data_type, this);
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

	// Initialize using the type of the base column
	state.Initialize(state.context, base_column->type, state.scan_options);
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
	Reassemble(scan_chunk.data[0], result, scan_count, geom_type, vert_type, 0);
	return scan_count;
}

idx_t GeoColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t target_count, idx_t result_offset) {
	auto &layout_type = base_column->GetType();

	// Not a shredded column, so just emit the binary format immediately
	if (layout_type.id() == LogicalTypeId::GEOMETRY) {
		return base_column->ScanCount(state, result, target_count, result_offset);
	}

	// Setup an intermediate chunk to scan the actual data, based on how much we actually scanned
	// TODO: Put this in a scan state
	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), {layout_type}, target_count);

	const auto scan_count = base_column->ScanCount(state, scan_chunk.data[0], target_count, 0);

	// Now reassemble
	Reassemble(scan_chunk.data[0], result, scan_count, geom_type, vert_type, result_offset);
	return scan_count;
}

void GeoColumnData::Skip(ColumnScanState &state, idx_t count) {
	return base_column->Skip(state, count);
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
	auto &layout_type = base_column->GetType();

	// Not a shredded column, so just emit the binary format immediately
	if (layout_type.id() == LogicalTypeId::GEOMETRY) {
		return base_column->Fetch(state, row_id, result);
	}

	// Otherwise, we need to fetch and reassemble
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {layout_type}, 1);

	const auto fetch_count = base_column->Fetch(state, row_id, chunk.data[0]);

	Reassemble(chunk.data[0], result, fetch_count, geom_type, vert_type, 0);

	return fetch_count;
}

void GeoColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, const StorageIndex &storage_index,
                             row_t row_id, Vector &result, idx_t result_idx) {
	auto &layout_type = base_column->GetType();

	// Not a shredded column, so just emit the binary format immediately
	if (layout_type.id() == LogicalTypeId::GEOMETRY) {
		return base_column->FetchRow(transaction, state, storage_index, row_id, result, result_idx);
	}

	// Otherwise, we need to fetch and reassemble
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {layout_type}, 1);

	base_column->FetchRow(transaction, state, storage_index, row_id, chunk.data[0], 0);

	Reassemble(chunk.data[0], result, 1, geom_type, vert_type, result_idx);
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

class GeoColumnCheckpointState final : public ColumnCheckpointState {
public:
	GeoColumnCheckpointState(const RowGroup &row_group, ColumnData &column_data,
	                         PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		// Make stats
		global_stats = GeometryStats::CreateEmpty(column_data.type).ToUnique();

		// Also pass on the shredding state
		const auto &geo_column = column_data.Cast<GeoColumnData>();
		geom_type = geo_column.geom_type;
		vert_type = geo_column.vert_type;
	}

	// Shared pointer to the new/old inner column.
	// This is never actually used here, but needs to stay alive
	// for as long as the checkpoint state, hence the shared_ptr.
	shared_ptr<ColumnData> inner_column;

	// The checkpoint state for the inner column.
	unique_ptr<ColumnCheckpointState> inner_column_state;

	GeometryType geom_type = GeometryType::INVALID;
	VertexType vert_type = VertexType::XY;

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

		// Pass on the shredding state too
		column_data.geom_type = geom_type;
		column_data.vert_type = vert_type;

		return ColumnCheckpointState::GetFinalResult();
	}

	unique_ptr<BaseStatistics> GetStatistics() override {
		D_ASSERT(global_stats);
		return std::move(global_stats);
	}

	PersistentColumnData ToPersistentData() override {
		auto inner_data = inner_column_state->ToPersistentData();

		// If this is a shredded column, record it in the persistent data!
		if (geom_type != GeometryType::INVALID) {
			auto extra_data = make_uniq<GeometryPersistentColumnData>();
			extra_data->geom_type = geom_type;
			extra_data->vert_type = vert_type;
			inner_data.extra_data = std::move(extra_data);
		}

		return inner_data;
	}
};

unique_ptr<ColumnCheckpointState> GeoColumnData::CreateCheckpointState(const RowGroup &row_group,
                                                                       PartialBlockManager &partial_block_manager) {
	return make_uniq<GeoColumnCheckpointState>(row_group, *this, partial_block_manager);
}

unique_ptr<ColumnCheckpointState> GeoColumnData::Checkpoint(const RowGroup &row_group, ColumnCheckpointInfo &info,
                                                            const BaseStatistics &old_stats) {
	auto &partial_block_manager = info.GetPartialBlockManager();
	auto checkpoint_state = make_uniq<GeoColumnCheckpointState>(row_group, *this, partial_block_manager);

	auto &old_column_stats =
	    base_column->GetType().id() == LogicalTypeId::GEOMETRY ? old_stats : base_column->GetStatisticsRef();

	// Are there any changes?
	if (!HasAnyChanges()) {
		// No changes, keep column
		checkpoint_state->inner_column = base_column;
		checkpoint_state->inner_column_state =
		    checkpoint_state->inner_column->Checkpoint(row_group, info, old_column_stats);
		return std::move(checkpoint_state);
	}

	// Do we have enough rows to consider shredding?
	auto &table_info = row_group.GetTableInfo();
	auto &db = table_info.GetDB();

	const auto shredding_threshold = Settings::Get<GeometryMinimumShreddingSize>(DBConfig::Get(db));
	const auto current_row_count = count.load();

	auto should_shred = shredding_threshold >= 0 && current_row_count >= static_cast<idx_t>(shredding_threshold);
	if (!should_shred) {
		// Keep column
		checkpoint_state->inner_column = base_column;
		checkpoint_state->inner_column_state =
		    checkpoint_state->inner_column->Checkpoint(row_group, info, old_column_stats);
		checkpoint_state->global_stats = checkpoint_state->inner_column_state->GetStatistics();
		return std::move(checkpoint_state);
	}

	// Figure out if this segment can use an alternative type layout
	auto new_geom_type = GeometryType::POINT;
	auto new_vert_type = VertexType::XY;

	const auto &types = GeometryStats::GetTypes(old_stats);
	const auto &flags = GeometryStats::GetFlags(old_stats);

	auto has_mixed_type = !types.TryGetSingleType(new_geom_type, new_vert_type);
	auto has_only_geometry_collection = new_geom_type == GeometryType::GEOMETRYCOLLECTION;
	auto has_only_invalid = new_geom_type == GeometryType::INVALID;

	// We cant specialize empty geometries, because we cant represent zero-vertex geometries in those layouts
	const auto has_empty = flags.HasEmptyGeometry() || flags.HasEmptyPart();

	if (has_mixed_type || has_only_geometry_collection || has_only_invalid || has_empty) {
		// Cant specialize, keep column
		checkpoint_state->inner_column = base_column;
		checkpoint_state->inner_column_state =
		    checkpoint_state->inner_column->Checkpoint(row_group, info, old_column_stats);
		checkpoint_state->global_stats = checkpoint_state->inner_column_state->GetStatistics();
		return std::move(checkpoint_state);
	}

	auto new_type = Geometry::GetVectorizedType(new_geom_type, new_vert_type);

	auto new_column = CreateColumn(block_manager, this->info, base_column->column_index, new_type, GetDataType(), this);

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
		Scan(TransactionData::Committed(), vector_index++, scan_state, scan_chunk.data[0], to_scan);

		// Verify the scan chunk
		scan_chunk.Verify();

		append_chunk.Reset();
		append_chunk.SetCardinality(to_scan);

		// Make the split
		Specialize(scan_chunk.data[0], append_chunk.data[0], to_scan, new_geom_type, new_vert_type);

		// Append into the new specialized column
		auto dummy_stats = BaseStatistics::CreateEmpty(new_column->GetType());
		new_column->Append(dummy_stats, append_state, append_chunk.data[0], to_scan);

		// Merge the stats into the checkpoint state's global stats
		InterpretStats(dummy_stats, *checkpoint_state->global_stats, new_geom_type, new_vert_type);
	}

	// Move then new column into our checkpoint state
	auto empty_stats = BaseStatistics::CreateEmpty(new_column->GetType());
	checkpoint_state->inner_column = new_column;
	checkpoint_state->inner_column_state = checkpoint_state->inner_column->Checkpoint(row_group, info, empty_stats);

	// Also set the shredding state
	checkpoint_state->geom_type = new_geom_type;
	checkpoint_state->vert_type = new_vert_type;

	return std::move(checkpoint_state);
}

bool GeoColumnData::IsPersistent() {
	return base_column->IsPersistent();
}

bool GeoColumnData::HasAnyChanges() const {
	return base_column->HasAnyChanges();
}

PersistentColumnData GeoColumnData::Serialize() {
	// Serialize the inner column
	auto inner_data = base_column->Serialize();

	// If this is a shredded column, record it in the persistent data!
	if (geom_type != GeometryType::INVALID) {
		auto extra_data = make_uniq<GeometryPersistentColumnData>();
		extra_data->geom_type = geom_type;
		extra_data->vert_type = vert_type;
		inner_data.extra_data = std::move(extra_data);
	}

	return inner_data;
}

void GeoColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	if (!column_data.extra_data) {
		// No shredding, just initialize normally
		base_column->InitializeColumn(column_data, target_stats);
		count = base_column->count.load();
		return;
	}

	auto &geom_data = column_data.extra_data->Cast<GeometryPersistentColumnData>();

	// Set the shredding state
	vert_type = geom_data.vert_type;
	geom_type = geom_data.geom_type;

	// Else, this is a shredded point
	const auto layout_type = Geometry::GetVectorizedType(geom_type, vert_type);
	base_column = CreateColumn(block_manager, info, base_column->column_index, layout_type, GetDataType(), this);
	D_ASSERT(base_column != nullptr);

	auto dummy_stats = BaseStatistics::CreateEmpty(layout_type);
	base_column->InitializeColumn(column_data, dummy_stats);
	count = base_column->count.load();

	// Interpret the stats
	InterpretStats(dummy_stats, target_stats, geom_type, vert_type);
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

void GeoColumnData::VisitBlockIds(BlockIdVisitor &visitor) const {
	return base_column->VisitBlockIds(visitor);
}

//----------------------------------------------------------------------------------------------------------------------
// Specialize
//----------------------------------------------------------------------------------------------------------------------
void GeoColumnData::Specialize(Vector &source, Vector &target, idx_t count, GeometryType geom_type,
                               VertexType vert_type) {
	Geometry::ToVectorizedFormat(source, target, count, geom_type, vert_type);
}

void GeoColumnData::Reassemble(Vector &source, Vector &target, idx_t count, GeometryType geom_type,
                               VertexType vert_type, idx_t result_offset) {
	Geometry::FromVectorizedFormat(source, target, count, geom_type, vert_type, result_offset);
}

static const BaseStatistics *GetVertexStats(BaseStatistics &stats, GeometryType geom_type) {
	switch (geom_type) {
	case GeometryType::POINT: {
		return StructStats::GetChildStats(stats);
	}
	case GeometryType::LINESTRING: {
		const auto &line_stats = ListStats::GetChildStats(stats);
		return StructStats::GetChildStats(line_stats);
	}
	case GeometryType::POLYGON: {
		const auto &poly_stats = ListStats::GetChildStats(stats);
		const auto &ring_stats = ListStats::GetChildStats(poly_stats);
		return StructStats::GetChildStats(ring_stats);
	}
	case GeometryType::MULTIPOINT: {
		const auto &mpoint_stats = ListStats::GetChildStats(stats);
		return StructStats::GetChildStats(mpoint_stats);
	}
	case GeometryType::MULTILINESTRING: {
		const auto &mline_stats = ListStats::GetChildStats(stats);
		const auto &line_stats = ListStats::GetChildStats(mline_stats);
		return StructStats::GetChildStats(line_stats);
	}
	case GeometryType::MULTIPOLYGON: {
		const auto &mpoly_stats = ListStats::GetChildStats(stats);
		const auto &poly_stats = ListStats::GetChildStats(mpoly_stats);
		const auto &ring_stats = ListStats::GetChildStats(poly_stats);
		return StructStats::GetChildStats(ring_stats);
	}
	default:
		throw NotImplementedException("Unsupported geometry type %d for interpreting stats",
		                              static_cast<int>(geom_type));
	}
}

void GeoColumnData::InterpretStats(BaseStatistics &source, BaseStatistics &target, GeometryType geom_type,
                                   VertexType vert_type) {
	// Copy base stats
	target.CopyBase(source);

	// Extract vertex stats
	const auto vert_stats = GetVertexStats(source, geom_type);
	auto &extent = GeometryStats::GetExtent(target);
	extent.x_min = NumericStats::GetMin<double>(vert_stats[0]);
	extent.x_max = NumericStats::GetMax<double>(vert_stats[0]);
	extent.y_min = NumericStats::GetMin<double>(vert_stats[1]);
	extent.y_max = NumericStats::GetMax<double>(vert_stats[1]);

	switch (vert_type) {
	case VertexType::XYZ:
		extent.z_min = NumericStats::GetMin<double>(vert_stats[2]);
		extent.z_max = NumericStats::GetMax<double>(vert_stats[2]);
		break;
	case VertexType::XYM:
		extent.m_min = NumericStats::GetMin<double>(vert_stats[2]);
		extent.m_max = NumericStats::GetMax<double>(vert_stats[2]);
		break;
	case VertexType::XYZM:
		extent.z_min = NumericStats::GetMin<double>(vert_stats[2]);
		extent.z_max = NumericStats::GetMax<double>(vert_stats[2]);
		extent.m_min = NumericStats::GetMin<double>(vert_stats[3]);
		extent.m_max = NumericStats::GetMax<double>(vert_stats[3]);
		break;
	default:
		// Nothing to do
		break;
	}

	// Set types
	auto &types = GeometryStats::GetTypes(target);
	types.Clear();
	types.Add(geom_type, vert_type);

	// Also set non-empty flag
	auto &geo_flags = GeometryStats::GetFlags(target);
	geo_flags.Clear();
	geo_flags.SetHasNonEmptyGeometry();
	geo_flags.SetHasNonEmptyPart();
}

} // namespace duckdb
