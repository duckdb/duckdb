#include "duckdb/storage/table/variant_column_data.hpp"
#include "duckdb/storage/table/struct_column_data.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/statistics/variant_stats.hpp"
#include "duckdb/function/variant/variant_shredding.hpp"

namespace duckdb {

VariantColumnData::VariantColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                     LogicalType type_p, ColumnDataType data_type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, std::move(type_p), data_type, parent),
      validity(block_manager, info, 0, *this) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);

	// the sub column index, starting at 1 (0 is the validity mask)
	idx_t sub_column_index = 1;
	auto unshredded_type = VariantShredding::GetUnshreddedType();
	sub_columns.push_back(
	    ColumnData::CreateColumnUnique(block_manager, info, sub_column_index++, unshredded_type, data_type, this));
}

void VariantColumnData::ReplaceColumns(unique_ptr<ColumnData> &&unshredded, unique_ptr<ColumnData> &&shredded) {
	for (auto &sub_column : sub_columns) {
		sub_column->CommitDropColumn();
	}

	sub_columns.clear();
	sub_columns.push_back(std::move(unshredded));
	sub_columns.push_back(std::move(shredded));
	is_shredded = true;
}

void VariantColumnData::CreateScanStates(ColumnScanState &state) {
	//! Re-initialize the scan state, since VARIANT can have a different shape for every RowGroup
	state.child_states.clear();

	state.child_states.emplace_back(state.parent);
	state.child_states[0].scan_options = state.scan_options;

	auto unshredded_type = VariantShredding::GetUnshreddedType();
	state.child_states.emplace_back(state.parent);
	state.child_states[1].Initialize(state.context, unshredded_type, state.scan_options);
	if (is_shredded) {
		auto &shredded_column = sub_columns[1];
		state.child_states.emplace_back(state.parent);
		state.child_states[2].Initialize(state.context, shredded_column->type, state.scan_options);
	}
}

idx_t VariantColumnData::GetMaxEntry() {
	return sub_columns[0]->GetMaxEntry();
}

void VariantColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) {
	validity.InitializePrefetch(prefetch_state, scan_state.child_states[0], rows);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->InitializePrefetch(prefetch_state, scan_state.child_states[i + 1], rows);
	}
}

void VariantColumnData::InitializeScan(ColumnScanState &state) {
	CreateScanStates(state);
	state.current = nullptr;

	// initialize the validity segment
	validity.InitializeScan(state.child_states[0]);

	// initialize the sub-columns
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->InitializeScan(state.child_states[i + 1]);
	}
}

void VariantColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	CreateScanStates(state);
	state.current = nullptr;

	// initialize the validity segment
	validity.InitializeScanWithOffset(state.child_states[0], row_idx);

	// initialize the sub-columns
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->InitializeScanWithOffset(state.child_states[i + 1], row_idx);
	}
}

Vector VariantColumnData::CreateUnshreddingIntermediate(idx_t count) {
	D_ASSERT(is_shredded);
	D_ASSERT(sub_columns.size() == 2);

	child_list_t<LogicalType> child_types;
	child_types.emplace_back("unshredded", sub_columns[0]->type);
	child_types.emplace_back("shredded", sub_columns[1]->type);
	auto intermediate_type = LogicalType::STRUCT(child_types);
	Vector intermediate(intermediate_type, count);
	return intermediate;
}

idx_t VariantColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                              idx_t target_count) {
	if (is_shredded) {
		auto intermediate = CreateUnshreddingIntermediate(target_count);
		auto &child_vectors = StructVector::GetEntries(intermediate);
		sub_columns[0]->Scan(transaction, vector_index, state.child_states[1], *child_vectors[0], target_count);
		sub_columns[1]->Scan(transaction, vector_index, state.child_states[2], *child_vectors[1], target_count);
		auto scan_count = validity.Scan(transaction, vector_index, state.child_states[0], intermediate, target_count);

		VariantColumnData::UnshredVariantData(intermediate, result, target_count);
		return scan_count;
	}
	auto scan_count = validity.Scan(transaction, vector_index, state.child_states[0], result, target_count);
	sub_columns[0]->Scan(transaction, vector_index, state.child_states[1], result, target_count);
	return scan_count;
}

idx_t VariantColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
                                       idx_t target_count) {
	if (is_shredded) {
		auto intermediate = CreateUnshreddingIntermediate(target_count);

		auto &child_vectors = StructVector::GetEntries(intermediate);
		sub_columns[0]->ScanCommitted(vector_index, state.child_states[1], *child_vectors[0], allow_updates,
		                              target_count);
		sub_columns[1]->ScanCommitted(vector_index, state.child_states[2], *child_vectors[1], allow_updates,
		                              target_count);
		auto scan_count =
		    validity.ScanCommitted(vector_index, state.child_states[0], intermediate, allow_updates, target_count);

		VariantColumnData::UnshredVariantData(intermediate, result, target_count);
		return scan_count;
	}
	auto scan_count =
	    sub_columns[0]->ScanCommitted(vector_index, state.child_states[1], result, allow_updates, target_count);
	return scan_count;
}

idx_t VariantColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset) {
	auto scan_count = sub_columns[0]->ScanCount(state.child_states[1], result, count, result_offset);
	return scan_count;
}

void VariantColumnData::Skip(ColumnScanState &state, idx_t count) {
	validity.Skip(state.child_states[0], count);

	// skip inside the sub-columns
	for (idx_t child_idx = 0; child_idx < sub_columns.size(); child_idx++) {
		sub_columns[child_idx]->Skip(state.child_states[child_idx + 1], count);
	}
}

void VariantColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnAppendState validity_append;
	validity.InitializeAppend(validity_append);
	state.child_appends.push_back(std::move(validity_append));

	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto &sub_column = sub_columns[i];
		ColumnAppendState child_append;
		sub_column->InitializeAppend(child_append);
		state.child_appends.push_back(std::move(child_append));
	}
}

namespace {

struct VariantShreddedAppendInput {
	ColumnData &unshredded;
	ColumnData &shredded;
	ColumnAppendState &unshredded_append_state;
	ColumnAppendState &shredded_append_state;
	BaseStatistics &unshredded_stats;
	BaseStatistics &shredded_stats;
};

} // namespace

static void AppendShredded(Vector &input, Vector &append_vector, idx_t count, VariantShreddedAppendInput &append_data) {
	D_ASSERT(append_vector.GetType().id() == LogicalTypeId::STRUCT);
	auto &child_vectors = StructVector::GetEntries(append_vector);
	D_ASSERT(child_vectors.size() == 2);

	//! Create the new column data for the shredded data
	VariantColumnData::ShredVariantData(input, append_vector, count);
	auto &unshredded_vector = *child_vectors[0];
	auto &shredded_vector = *child_vectors[1];

	auto &unshredded = append_data.unshredded;
	auto &shredded = append_data.shredded;

	auto &unshredded_stats = append_data.unshredded_stats;
	auto &shredded_stats = append_data.shredded_stats;

	auto &unshredded_append_state = append_data.unshredded_append_state;
	auto &shredded_append_state = append_data.shredded_append_state;

	unshredded.Append(unshredded_stats, unshredded_append_state, unshredded_vector, count);
	shredded.Append(shredded_stats, shredded_append_state, shredded_vector, count);
}

void VariantColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
		Vector append_vector(vector);
		append_vector.Flatten(count);
		Append(stats, state, append_vector, count);
		return;
	}

	// append the null values
	validity.Append(stats, state.child_appends[0], vector, count);

	if (is_shredded) {
		auto &unshredded_type = sub_columns[0]->type;
		auto &shredded_type = sub_columns[1]->type;

		auto variant_shredded_type = LogicalType::STRUCT({
		    {"unshredded", unshredded_type},
		    {"shredded", shredded_type},
		});
		Vector append_vector(variant_shredded_type, count);

		VariantShreddedAppendInput append_data {
		    *sub_columns[0],
		    *sub_columns[1],
		    state.child_appends[1],
		    state.child_appends[2],
		    VariantStats::GetUnshreddedStats(stats),
		    VariantStats::GetShreddedStats(stats),
		};
		AppendShredded(vector, append_vector, count, append_data);
	} else {
		for (idx_t i = 0; i < sub_columns.size(); i++) {
			sub_columns[i]->Append(VariantStats::GetUnshreddedStats(stats), state.child_appends[i + 1], vector, count);
		}
	}
	this->count += count;
}

void VariantColumnData::RevertAppend(row_t new_count) {
	validity.RevertAppend(new_count);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto &sub_column = sub_columns[i];
		sub_column->RevertAppend(new_count);
	}
	this->count = UnsafeNumericCast<idx_t>(new_count);
}

idx_t VariantColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("VARIANT Fetch");
}

void VariantColumnData::Update(TransactionData transaction, DataTable &data_table, idx_t column_index,
                               Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t row_group_start) {
	throw NotImplementedException("VARIANT Update is not supported.");
}

void VariantColumnData::UpdateColumn(TransactionData transaction, DataTable &data_table,
                                     const vector<column_t> &column_path, Vector &update_vector, row_t *row_ids,
                                     idx_t update_count, idx_t depth, idx_t row_group_start) {
	throw NotImplementedException("VARIANT Update Column is not supported");
}

unique_ptr<BaseStatistics> VariantColumnData::GetUpdateStatistics() {
	return nullptr;
}

void VariantColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                                 idx_t result_idx) {
	// insert any child states that are required
	for (idx_t i = state.child_states.size(); i < sub_columns.size() + 1; i++) {
		auto child_state = make_uniq<ColumnFetchState>();
		state.child_states.push_back(std::move(child_state));
	}

	if (is_shredded) {
		auto intermediate = CreateUnshreddingIntermediate(result_idx + 1);
		auto &child_vectors = StructVector::GetEntries(intermediate);
		// fetch the validity state
		validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);
		// fetch the sub-column states
		for (idx_t i = 0; i < sub_columns.size(); i++) {
			sub_columns[i]->FetchRow(transaction, *state.child_states[i + 1], row_id, *child_vectors[i], result_idx);
		}
		if (result_idx) {
			intermediate.SetValue(0, intermediate.GetValue(result_idx));
		}

		//! FIXME: adjust UnshredVariantData so we can write the value in place into 'result' directly.
		Vector unshredded(result.GetType(), 1);
		VariantColumnData::UnshredVariantData(intermediate, unshredded, 1);
		result.SetValue(result_idx, unshredded.GetValue(0));
		return;
	}

	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);
	sub_columns[0]->FetchRow(transaction, *state.child_states[1], row_id, result, result_idx);
}

void VariantColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto &sub_column = sub_columns[i];
		sub_column->CommitDropColumn();
	}
}

struct VariantColumnCheckpointState : public ColumnCheckpointState {
	VariantColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
	                             PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		global_stats = VariantStats::CreateEmpty(column_data.type).ToUnique();
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	vector<unique_ptr<ColumnCheckpointState>> child_states;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		D_ASSERT(global_stats);
		VariantStats::SetUnshreddedStats(*global_stats, child_states[0]->GetStatistics());
		if (child_states.size() == 2) {
			VariantStats::SetShreddedStats(*global_stats, child_states[1]->GetStatistics());
		}
		return std::move(global_stats);
	}

	PersistentColumnData ToPersistentData() override {
		PersistentColumnData data(column_data.type);
		auto &variant_column_data = column_data.Cast<VariantColumnData>();
		if (child_states.size() == 2) {
			D_ASSERT(variant_column_data.sub_columns.size() == 2);
			D_ASSERT(variant_column_data.sub_columns[1]->type.id() == LogicalTypeId::STRUCT);
			data.SetVariantShreddedType(variant_column_data.sub_columns[1]->type);
		}
		data.child_columns.push_back(validity_state->ToPersistentData());
		for (auto &child_state : child_states) {
			data.child_columns.push_back(child_state->ToPersistentData());
		}
		return data;
	}
};

unique_ptr<ColumnCheckpointState> VariantColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                           PartialBlockManager &partial_block_manager) {
	return make_uniq<VariantColumnCheckpointState>(row_group, *this, partial_block_manager);
}

vector<unique_ptr<ColumnData>> VariantColumnData::WriteShreddedData(RowGroup &row_group,
                                                                    const LogicalType &shredded_type) {
	//! scan_chunk
	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::VARIANT()}, STANDARD_VECTOR_SIZE);
	auto &scan_vector = scan_chunk.data[0];

	//! append_chunk
	auto &child_types = StructType::GetChildTypes(shredded_type);

	DataChunk append_chunk;
	append_chunk.Initialize(Allocator::DefaultAllocator(), {shredded_type}, STANDARD_VECTOR_SIZE);
	auto &append_vector = append_chunk.data[0];

	//! Create the new column data for the shredded data
	D_ASSERT(child_types.size() == 2);
	auto &unshredded_type = child_types[0].second;
	auto &typed_value_type = child_types[1].second;

	vector<unique_ptr<ColumnData>> ret(2);
	ret[0] = CreateColumnUnique(block_manager, info, 1, unshredded_type, GetDataType(), this);
	ret[1] = CreateColumnUnique(block_manager, info, 2, typed_value_type, GetDataType(), this);
	auto &unshredded = ret[0];
	auto &shredded = ret[1];

	ColumnAppendState unshredded_append_state;
	unshredded->InitializeAppend(unshredded_append_state);

	ColumnAppendState shredded_append_state;
	shredded->InitializeAppend(shredded_append_state);

	ColumnScanState scan_state(nullptr);

	InitializeScan(scan_state);
	//! Scan + transform + append
	idx_t total_count = count.load();

	auto transformed_stats = VariantStats::CreateShredded(typed_value_type);
	auto &unshredded_stats = VariantStats::GetUnshreddedStats(transformed_stats);
	auto &shredded_stats = VariantStats::GetShreddedStats(transformed_stats);

	VariantShreddedAppendInput append_data {*unshredded,           *shredded,        unshredded_append_state,
	                                        shredded_append_state, unshredded_stats, shredded_stats};
	idx_t vector_index = 0;
	for (idx_t scanned = 0; scanned < total_count; scanned += STANDARD_VECTOR_SIZE) {
		scan_chunk.Reset();
		auto to_scan = MinValue(total_count - scanned, static_cast<idx_t>(STANDARD_VECTOR_SIZE));
		ScanCommitted(vector_index++, scan_state, scan_vector, false, to_scan);
		append_chunk.Reset();

		AppendShredded(scan_vector, append_vector, to_scan, append_data);
	}
	stats->statistics = std::move(transformed_stats);
	return ret;
}

LogicalType VariantColumnData::GetShreddedType() {
	VariantShreddingStats variant_stats;

	//! scan_chunk
	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::VARIANT()}, STANDARD_VECTOR_SIZE);
	auto &scan_vector = scan_chunk.data[0];

	ColumnScanState scan_state(nullptr);
	InitializeScan(scan_state);
	idx_t total_count = count.load();
	idx_t vector_index = 0;
	for (idx_t scanned = 0; scanned < total_count; scanned += STANDARD_VECTOR_SIZE) {
		scan_chunk.Reset();
		auto to_scan = MinValue(total_count - scanned, static_cast<idx_t>(STANDARD_VECTOR_SIZE));
		ScanCommitted(vector_index++, scan_state, scan_vector, false, to_scan);
		variant_stats.Update(scan_vector, to_scan);
	}

	return variant_stats.GetShreddedType();
}

unique_ptr<ColumnCheckpointState> VariantColumnData::Checkpoint(RowGroup &row_group,
                                                                ColumnCheckpointInfo &checkpoint_info) {
	auto &partial_block_manager = checkpoint_info.GetPartialBlockManager();
	auto checkpoint_state = make_uniq<VariantColumnCheckpointState>(row_group, *this, partial_block_manager);
	checkpoint_state->validity_state = validity.Checkpoint(row_group, checkpoint_info);

	if (!HasAnyChanges()) {
		for (idx_t i = 0; i < sub_columns.size(); i++) {
			checkpoint_state->child_states.push_back(sub_columns[i]->Checkpoint(row_group, checkpoint_info));
		}
		return std::move(checkpoint_state);
	}

	auto shredded_type = GetShreddedType();
	D_ASSERT(shredded_type.id() == LogicalTypeId::STRUCT);
	auto &type_entries = StructType::GetChildTypes(shredded_type);
	if (type_entries.size() == 2) {
		//! STRUCT(unshredded VARIANT, shredded <...>)
		auto shredded_data = WriteShreddedData(row_group, shredded_type);
		D_ASSERT(shredded_data.size() == 2);
		auto &unshredded = shredded_data[0];
		auto &shredded = shredded_data[1];

		//! Now checkpoint the shredded data
		checkpoint_state->child_states.push_back(unshredded->Checkpoint(row_group, checkpoint_info));
		checkpoint_state->child_states.push_back(shredded->Checkpoint(row_group, checkpoint_info));

		//! Replace the old data with the new
		ReplaceColumns(std::move(unshredded), std::move(shredded));
	} else {
		D_ASSERT(type_entries.size() == 1);
		//! STRUCT(unshredded VARIANT)
		checkpoint_state->child_states.push_back(sub_columns[0]->Checkpoint(row_group, checkpoint_info));
	}

	return std::move(checkpoint_state);
}

bool VariantColumnData::IsPersistent() {
	if (!validity.IsPersistent()) {
		return false;
	}
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto &sub_column = sub_columns[i];
		if (!sub_column->IsPersistent()) {
			return false;
		}
	}
	return true;
}

bool VariantColumnData::HasAnyChanges() const {
	if (validity.HasAnyChanges()) {
		return true;
	}
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto &sub_column = sub_columns[i];
		if (sub_column->HasAnyChanges()) {
			return true;
		}
	}
	return false;
}

PersistentColumnData VariantColumnData::Serialize() {
	PersistentColumnData persistent_data(type);
	if (is_shredded) {
		persistent_data.SetVariantShreddedType(sub_columns[1]->type);
	}
	persistent_data.child_columns.push_back(validity.Serialize());
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto &sub_column = sub_columns[i];
		persistent_data.child_columns.push_back(sub_column->Serialize());
	}
	return persistent_data;
}

void VariantColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	validity.InitializeColumn(column_data.child_columns[0], target_stats);

	if (column_data.child_columns.size() == 3) {
		//! This means the VARIANT is shredded
		auto &unshredded_stats = VariantStats::GetUnshreddedStats(target_stats);
		sub_columns[0]->InitializeColumn(column_data.child_columns[1], unshredded_stats);

		auto &shredded_type = column_data.variant_shredded_type;
		if (!is_shredded) {
			VariantStats::SetShreddedStats(target_stats, BaseStatistics::CreateEmpty(shredded_type));
			sub_columns.push_back(
			    ColumnData::CreateColumnUnique(block_manager, info, 2, shredded_type, GetDataType(), this));
			is_shredded = true;
		}
		auto &shredded_stats = VariantStats::GetShreddedStats(target_stats);
		sub_columns[1]->InitializeColumn(column_data.child_columns[2], shredded_stats);
	} else {
		auto &unshredded_stats = VariantStats::GetUnshreddedStats(target_stats);
		sub_columns[0]->InitializeColumn(column_data.child_columns[1], unshredded_stats);
	}
	this->count = validity.count.load();
}

void VariantColumnData::GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index, vector<idx_t> col_path,
                                             vector<ColumnSegmentInfo> &result) {
	col_path.push_back(0);
	validity.GetColumnSegmentInfo(context, row_group_index, col_path, result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		col_path.back() = i + 1;
		sub_columns[i]->GetColumnSegmentInfo(context, row_group_index, col_path, result);
	}
}

void VariantColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity.Verify(parent);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto &sub_column = sub_columns[i];
		sub_column->Verify(parent);
	}
#endif
}

} // namespace duckdb
