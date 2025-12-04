#include "duckdb/storage/table/geo_column_data.hpp"

#include "duckdb/storage/table/struct_column_data.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

static LogicalType GetUnshreddedType() {
	return LogicalType::GEOMETRY();
}

GeoColumnData::GeoColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, LogicalType type,
                             ColumnDataType data_type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, std::move(type), data_type, parent) {
	// Create the validity column
	if (data_type != ColumnDataType::CHECKPOINT_TARGET) {
		validity = make_shared_ptr<ValidityColumnData>(block_manager, info, 0, *this);

		// Create the BLOB column

		// We do this manually to avoid infinite recursion
		blob_column = make_shared_ptr<StandardColumnData>(block_manager, info, 1, type, data_type, this);

		// Make stats for these too, even though they technically have a parent already
		blob_column->stats = make_uniq<SegmentStatistics>(blob_column->type);

	} else {
		// Leave empty, gets populated by 'SetBlobData' and 'SetGeomData'
	}
}

void GeoColumnData::SetValidityData(shared_ptr<ValidityColumnData> validity_p) {
	if (validity) {
		throw InternalException("GeoColumnData::SetBlobColumnData cannot be used to overwrite existing blob column");
	}
	validity_p->SetParent(this);
	validity = std::move(validity_p);
}

void GeoColumnData::SetBlobColumnData(shared_ptr<ColumnData> blob_column_p) {
	if (blob_column) {
		throw InternalException("GeoColumnData::SetBlobColumnData cannot be used to overwrite existing blob column");
	}
	blob_column_p->SetParent(this);
	blob_column = std::move(blob_column_p);
}

void GeoColumnData::SetGeomColumnData(shared_ptr<ColumnData> geom_column_p) {
	if (geom_column) {
		throw InternalException(
		    "GeoColumnData::SetGeomColumnData cannot be used to overwrite existing geometry column");
	}
	geom_column_p->SetParent(this);
	geom_column = std::move(geom_column_p);
}

void GeoColumnData::CreateScanStates(ColumnScanState &state) {
	//! Re-initialize the scan state, since there can be different shapes for every RowGroup
	state.child_states.clear();

	// Initialize validity
	state.child_states.emplace_back(state.parent);
	state.child_states[0].scan_options = state.scan_options;

	// Initialize blob column
	state.child_states.emplace_back(state.parent);
	state.child_states[1].Initialize(state.context, GetUnshreddedType(), state.scan_options);

	// Initialize geometry column if shredded
	if (IsShredded()) {
		const auto &shredded_column = geom_column;
		state.child_states.emplace_back(state.parent);
		state.child_states[2].Initialize(state.context, shredded_column->type, state.scan_options);
	}
}

idx_t GeoColumnData::GetMaxEntry() {
	return blob_column->GetMaxEntry();
}

void GeoColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) {
	validity->InitializePrefetch(prefetch_state, scan_state.child_states[0], rows);
	blob_column->InitializePrefetch(prefetch_state, scan_state.child_states[1], rows);

	if (IsShredded()) {
		geom_column->InitializePrefetch(prefetch_state, scan_state.child_states[2], rows);
	}
}

void GeoColumnData::InitializeScan(ColumnScanState &state) {
	GeoColumnData::CreateScanStates(state);

	state.current = nullptr;

	// Initialize validity
	validity->InitializeScan(state.child_states[0]);
	blob_column->InitializeScan(state.child_states[1]);
	if (IsShredded()) {
		geom_column->InitializeScan(state.child_states[2]);
	}
}

void GeoColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	GeoColumnData::CreateScanStates(state);

	state.current = nullptr;

	// Initialize validity
	validity->InitializeScanWithOffset(state.child_states[0], row_idx);
	blob_column->InitializeScanWithOffset(state.child_states[1], row_idx);
	if (IsShredded()) {
		geom_column->InitializeScanWithOffset(state.child_states[2], row_idx);
	}
}

idx_t GeoColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                          idx_t target_count) {
	if (IsShredded()) {
		// We need to scan and reassemble the decomposed segment too
		DataChunk scan_chunk;
		scan_chunk.Initialize(Allocator::DefaultAllocator(), {blob_column->type, geom_column->type}, target_count);

		// Now scan the blob and geom columns into the chunk
		blob_column->Scan(transaction, vector_index, state.child_states[1], scan_chunk.data[0], target_count);
		geom_column->Scan(transaction, vector_index, state.child_states[2], scan_chunk.data[1], target_count);

		auto scan_count = validity->Scan(transaction, vector_index, state.child_states[0], result, target_count);

		// Now reassemble the geometry data
		auto &blob_vector = scan_chunk.data[0];
		auto &geom_vector = scan_chunk.data[1];

		UnifiedVectorFormat blob_uvu;
		UnifiedVectorFormat geom_uvu;

		blob_vector.ToUnifiedFormat(target_count, blob_uvu);
		geom_vector.ToUnifiedFormat(target_count, geom_uvu);

		const auto &parts = StructVector::GetEntries(geom_vector);
		const auto &x_vec = *parts[0];
		const auto &y_vec = *parts[1];

		const auto result_data = FlatVector::GetData<string_t>(result);
		const auto blob_data = UnifiedVectorFormat::GetData<string_t>(blob_uvu);
		const auto x_data = FlatVector::GetData<double>(x_vec);
		const auto y_data = FlatVector::GetData<double>(y_vec);

		for (idx_t i = 0; i < target_count; i++) {
			const auto blob_idx = blob_uvu.sel->get_index(i);
			const auto geom_idx = geom_uvu.sel->get_index(i);
			const auto blob_valid = blob_uvu.validity.RowIsValid(blob_idx);
			const auto geom_valid = geom_uvu.validity.RowIsValid(geom_idx);

			if (blob_valid && !geom_valid) {
				result_data[i] = StringVector::AddStringOrBlob(result, blob_data[blob_idx]);
				continue;
			}

			if (!blob_valid && geom_valid) {
				char buffer[1 + 4 + 8 + 8];
				memcpy(buffer, "\x01\x01\x00\x00\x00", 5); // POINT type
				memcpy(buffer + 5, &x_data[geom_idx], 8);
				memcpy(buffer + 13, &y_data[geom_idx], 8);
				result_data[i] = StringVector::AddStringOrBlob(result, string_t(buffer, sizeof(buffer)));
				continue;
			}

			if (!blob_valid && !geom_valid) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			// Both cant be valid
			D_ASSERT(false);
		}

		return scan_count;
	}

	// No shredding, just scan the validity and blob columns
	const auto scan_count = validity->Scan(transaction, vector_index, state.child_states[0], result, target_count);
	blob_column->Scan(transaction, vector_index, state.child_states[1], result, target_count);
	return scan_count;
}

idx_t GeoColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
                                   idx_t target_count) {
	if (IsShredded()) {
		// We need to scan and reassemble the decomposed segment too
		throw NotImplementedException("Shredded GeoColumnData ScanCommitted is not implemented yet.");
	}

	// No shredding, just scan the validity and blob columns
	const auto scan_count =
	    validity->ScanCommitted(vector_index, state.child_states[0], result, allow_updates, target_count);
	blob_column->ScanCommitted(vector_index, state.child_states[1], result, allow_updates, target_count);
	return scan_count;
}

idx_t GeoColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset) {
	// TODO: Is this enough?
	return blob_column->ScanCount(state.child_states[1], result, count, result_offset);
}

void GeoColumnData::Skip(ColumnScanState &state, idx_t count) {
	validity->Skip(state.child_states[0], count);
	blob_column->Skip(state.child_states[1], count);
	if (IsShredded()) {
		geom_column->Skip(state.child_states[2], count);
	}
}

void GeoColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnAppendState validity_append;
	validity->InitializeAppend(validity_append);
	state.child_appends.push_back(std::move(validity_append));

	ColumnAppendState blob_append;
	blob_column->InitializeAppend(blob_append);
	state.child_appends.push_back(std::move(blob_append));

	if (IsShredded()) {
		ColumnAppendState geom_append;
		geom_column->InitializeAppend(geom_append);
		state.child_appends.push_back(std::move(geom_append));
	}
}

void GeoColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t row_count) {
	if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
		Vector append_vector(vector);
		append_vector.Flatten(row_count);
		Append(stats, state, append_vector, row_count);
		return;
	}

	validity->Append(stats, state.child_appends[0], vector, row_count);

	if (IsShredded()) {
		throw NotImplementedException("Shredded GeoColumnData Append is not implemented yet.");
	} else {
		// Mark as not shredded?
		blob_column->Append(stats, state.child_appends[1], vector, row_count);
	}

	count += row_count;
}

void GeoColumnData::RevertAppend(row_t new_count) {
	validity->RevertAppend(new_count);
	blob_column->RevertAppend(new_count);

	if (IsShredded()) {
		geom_column->RevertAppend(new_count);
	}

	count = UnsafeNumericCast<idx_t>(new_count);
}

idx_t GeoColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("GeoColumnData Fetch is not implemented yet.");
}

void GeoColumnData::Update(TransactionData transaction, DataTable &data_table, idx_t column_index,
                           Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t row_group_start) {
	throw NotImplementedException("GeoColumnData Update is not implemented yet.");
}

void GeoColumnData::UpdateColumn(TransactionData transaction, DataTable &data_table,
                                 const vector<column_t> &column_path, Vector &update_vector, row_t *row_ids,
                                 idx_t update_count, idx_t depth, idx_t row_group_start) {
	throw NotImplementedException("GeoColumnData UpdateColumn is not implemented yet.");
}

unique_ptr<BaseStatistics> GeoColumnData::GetUpdateStatistics() {
	return nullptr;
}

void GeoColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                             idx_t result_idx) {
}

void GeoColumnData::CommitDropColumn() {
	validity->CommitDropColumn();
	blob_column->CommitDropColumn();

	if (IsShredded()) {
		geom_column->CommitDropColumn();
	}
}

struct GeoColumnCheckpointState final : public ColumnCheckpointState {
	GeoColumnCheckpointState(const RowGroup &row_group, ColumnData &column_data,
	                         PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		global_stats = GeometryStats::CreateEmpty(column_data.type).ToUnique();
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	unique_ptr<ColumnCheckpointState> blob_state;
	unique_ptr<ColumnCheckpointState> geom_state;

	shared_ptr<ColumnData> blob_data;
	shared_ptr<ColumnData> geom_data;

public:
	shared_ptr<ColumnData> CreateEmptyColumnData() override {
		return make_shared_ptr<GeoColumnData>(original_column.GetBlockManager(), original_column.GetTableInfo(),
		                                      original_column.column_index, original_column.type,
		                                      ColumnDataType::CHECKPOINT_TARGET, nullptr);
	}

	shared_ptr<ColumnData> GetFinalResult() override {
		if (!result_column) {
			result_column = CreateEmptyColumnData();
		}

		auto &column_data = result_column->Cast<GeoColumnData>();

		auto validity_child = validity_state->GetFinalResult();
		column_data.SetValidityData(shared_ptr_cast<ColumnData, ValidityColumnData>(std::move(validity_child)));

		auto blob_child = blob_state->GetFinalResult();
		column_data.SetBlobColumnData(std::move(blob_child));

		// If shredded, set the geometry column data
		if (geom_state) {
			auto geom_child = geom_state->GetFinalResult();
			column_data.SetGeomColumnData(std::move(geom_child));
		}

		return ColumnCheckpointState::GetFinalResult();
	}

	unique_ptr<BaseStatistics> GetStatistics() override {
		D_ASSERT(global_stats);
		global_stats->Merge(*validity_state->GetStatistics());
		global_stats->Merge(*blob_state->GetStatistics());

		// TODO: Merge shredded stats too
		if (geom_state) {
			// throw NotImplementedException("GeoColumnCheckpointState GetStatistics for shredded data is not
			// implemented yet.");
		}

		return std::move(global_stats);
	}

	PersistentColumnData ToPersistentData() override {
		PersistentColumnData data(original_column.type);

		auto &geo_column_data = GetResultColumn().Cast<GeoColumnData>();

		data.child_columns.push_back(validity_state->ToPersistentData());
		data.child_columns.push_back(blob_state->ToPersistentData());

		if (geom_state) {
			// Save the shredded type here too
			data.SetVariantShreddedType(geo_column_data.geom_column->type);
			data.child_columns.push_back(geom_state->ToPersistentData());
		}

		return data;
	}
};

unique_ptr<ColumnCheckpointState> GeoColumnData::CreateCheckpointState(const RowGroup &row_group,
                                                                       PartialBlockManager &partial_block_manager) {
	return make_uniq<GeoColumnCheckpointState>(row_group, *this, partial_block_manager);
}

LogicalType GeoColumnData::GetShreddedType() {
	UnifiedVectorFormat v_format;
	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::GEOMETRY()}, STANDARD_VECTOR_SIZE);
	auto &scan_vector = scan_chunk.data[0];

	ColumnScanState scan_state(nullptr);
	InitializeScan(scan_state);

	auto scanned_stats = GeometryStats::CreateEmpty(type);

	idx_t total_count = count.load();
	idx_t vector_index = 0;
	for (idx_t scanned = 0; scanned < total_count; scanned += STANDARD_VECTOR_SIZE) {
		scan_chunk.Reset();
		auto to_scan = MinValue(total_count - scanned, static_cast<idx_t>(STANDARD_VECTOR_SIZE));
		ScanCommitted(vector_index++, scan_state, scan_vector, false, to_scan);

		// Now update the stats
		scan_vector.ToUnifiedFormat(to_scan, v_format);
		const auto v_data = UnifiedVectorFormat::GetData<string_t>(v_format);
		for (idx_t r_idx = 0; r_idx < to_scan; r_idx++) {
			const auto v_idx = v_format.sel->get_index(r_idx);
			if (!v_format.validity.RowIsValid(v_idx)) {
				continue;
			}
			GeometryStats::Update(scanned_stats, v_data[v_idx]);
		}
	}

	// If the stats only contain POINT XY, we can shred
	auto &types = GeometryStats::GetTypes(scanned_stats);
	if (types.HasOnly(GeometryType::POINT, VertexType::XY)) {
		return LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}});
	}

	return LogicalTypeId::INVALID;
}

pair<shared_ptr<ColumnData>, shared_ptr<ColumnData>>
GeoColumnData::WriteShreddedData(const RowGroup &row_group, const LogicalType &shredded_type, BaseStatistics &stats) {
	// scan chunk
	UnifiedVectorFormat v_format;

	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::GEOMETRY()}, STANDARD_VECTOR_SIZE);
	auto &scan_vector = scan_chunk.data[0];

	DataChunk append_chunk;
	append_chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::GEOMETRY(), shredded_type},
	                        STANDARD_VECTOR_SIZE);

	// Unshredded data
	// Make stats for these too, even though they technically have a parent already
	auto blob_column_data = make_shared_ptr<StandardColumnData>(block_manager, info, 1, type, GetDataType(), this);
	blob_column_data->stats = make_uniq<SegmentStatistics>(blob_column->type);

	// ColumnData::CreateColumn(block_manager, info, 1, LogicalType::GEOMETRY(), GetDataType(), this);
	ColumnAppendState blob_append_state;
	blob_column_data->InitializeAppend(blob_append_state);

	// Shredded data
	auto geom_column_data = ColumnData::CreateColumn(block_manager, info, 2, shredded_type, GetDataType(), this);
	ColumnAppendState geom_append_state;
	geom_column_data->InitializeAppend(geom_append_state);

	// Scan state for the original data
	ColumnScanState scan_state(nullptr);
	InitializeScan(scan_state);

	idx_t total_count = count.load();
	idx_t vector_index = 0;

	for (idx_t scanned = 0; scanned < total_count; scanned += STANDARD_VECTOR_SIZE) {
		scan_chunk.Reset();
		auto to_scan = MinValue(total_count - scanned, static_cast<idx_t>(STANDARD_VECTOR_SIZE));
		ScanCommitted(vector_index++, scan_state, scan_vector, false, to_scan);

		append_chunk.Reset();
		auto &blob_vector = append_chunk.data[0];
		auto &geom_vector = append_chunk.data[1];

		auto &parts = StructVector::GetEntries(geom_vector);
		auto &x_vec = *parts[0];
		auto &y_vec = *parts[1];
		auto x_data = FlatVector::GetData<double>(x_vec);
		auto y_data = FlatVector::GetData<double>(y_vec);

		auto blob_data = FlatVector::GetData<string_t>(blob_vector);

		// TODO: This is cheating and should be generalized:
		scan_vector.ToUnifiedFormat(to_scan, v_format);
		auto v_data = UnifiedVectorFormat::GetData<string_t>(v_format);

		for (idx_t r_idx = 0; r_idx < to_scan; r_idx++) {
			const auto v_idx = v_format.sel->get_index(r_idx);

			if (!v_format.validity.RowIsValid(v_idx)) {
				FlatVector::SetNull(blob_vector, r_idx, true);
				FlatVector::SetNull(geom_vector, r_idx, true);
				continue;
			}

			const auto &blob = v_data[v_idx];
			const auto type = Geometry::GetType(blob);
			if (type.first == GeometryType::POINT && type.second == VertexType::XY) {
				// Shred!
				auto data = blob.GetData();
				// X/Y is at (1 + 4) offset
				memcpy(&x_data[r_idx], data + sizeof(uint8_t) + sizeof(uint32_t), sizeof(double));
				memcpy(&y_data[r_idx], data + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(double), sizeof(double));

				FlatVector::SetNull(blob_vector, r_idx, true);
			} else {
				// Keep in BLOB
				blob_data[r_idx] = StringVector::AddStringOrBlob(blob_vector, blob);

				FlatVector::SetNull(geom_vector, r_idx, true);
			}
		}

		// Finally append
		// TODO: Move these stats somewhere else (or make them actually useful)
		auto blob_stats = GeometryStats::CreateEmpty(type);
		blob_column_data->Append(blob_stats, blob_append_state, blob_vector, to_scan);
		geom_column_data->Append(stats, geom_append_state, geom_vector, to_scan);
	}

	// return the stats
	return {std::move(blob_column_data), std::move(geom_column_data)};
}

unique_ptr<ColumnCheckpointState> GeoColumnData::Checkpoint(const RowGroup &row_group, ColumnCheckpointInfo &info) {
	auto &partial_block_manager = info.GetPartialBlockManager();
	auto checkpoint_state = make_uniq<GeoColumnCheckpointState>(row_group, *this, partial_block_manager);

	// Checkpoint validity
	checkpoint_state->validity_state = validity->Checkpoint(row_group, info);

	auto should_shred = true;

	if (!HasAnyChanges()) {
		should_shred = false;
	}

	// TODO: Determine if shredding
	LogicalType shredded_type;
	if (should_shred) {
		shredded_type = GetShreddedType();
		if (shredded_type.id() == LogicalTypeId::INVALID) {
			should_shred = false;
		}
	}

	if (!should_shred) {
		checkpoint_state->blob_state = blob_column->Checkpoint(row_group, info);
		return std::move(checkpoint_state);
	}

	// Otherwise, we need to checkpoint the shredded data
	auto column_stats = BaseStatistics::CreateEmpty(shredded_type);
	auto res = WriteShreddedData(row_group, shredded_type, column_stats);

	checkpoint_state->blob_data = std::move(res.first);
	checkpoint_state->geom_data = std::move(res.second);

	checkpoint_state->blob_state = checkpoint_state->blob_data->Checkpoint(row_group, info);
	checkpoint_state->geom_state = checkpoint_state->geom_data->Checkpoint(row_group, info);

	return std::move(checkpoint_state);
}

bool GeoColumnData::IsPersistent() {
	if (!validity->IsPersistent()) {
		return false;
	}

	if (!blob_column->IsPersistent()) {
		return false;
	}

	if (IsShredded()) {
		if (!geom_column->IsPersistent()) {
			return false;
		}
	}

	return true;
}

bool GeoColumnData::HasAnyChanges() const {
	if (validity->HasAnyChanges()) {
		return true;
	}
	if (blob_column->HasAnyChanges()) {
		return true;
	}
	if (IsShredded()) {
		if (geom_column->HasAnyChanges()) {
			return true;
		}
	}
	return false;
}

PersistentColumnData GeoColumnData::Serialize() {
	// Maybe make this a struct?
	PersistentColumnData persistent_data(type);

	persistent_data.child_columns.push_back(validity->Serialize());
	persistent_data.child_columns.push_back(blob_column->Serialize());

	if (IsShredded()) {
		persistent_data.SetVariantShreddedType(geom_column->type);
		persistent_data.child_columns.push_back(geom_column->Serialize());
	}

	return persistent_data;
}

void GeoColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	validity->InitializeColumn(column_data.child_columns[0], target_stats);

	if (column_data.child_columns.size() == 3) {
		// Shredded
		throw NotImplementedException("GeoColumnData InitializeColumn for shredded data is not implemented yet.");
	} else {
		// Since the BLOB column is a GEOMETRY type, we can directly pass the stats.
		// TODO: Fix this...?
		blob_column->InitializeColumn(column_data.child_columns[1], target_stats);
	}

	// Set the count
	count = validity->count.load();
}

void GeoColumnData::GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index, vector<idx_t> col_path,
                                         vector<ColumnSegmentInfo> &result) {
	col_path.push_back(0);
	;
	validity->GetColumnSegmentInfo(context, row_group_index, col_path, result);

	col_path.back() = 1;
	blob_column->GetColumnSegmentInfo(context, row_group_index, col_path, result);

	if (IsShredded()) {
		col_path.back() = 2;
		geom_column->GetColumnSegmentInfo(context, row_group_index, col_path, result);
	}
}

void GeoColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity->Verify(parent);
	blob_column->Verify(parent);
	if (IsShredded()) {
		geom_column->Verify(parent);
	}
#endif
}

} // namespace duckdb
