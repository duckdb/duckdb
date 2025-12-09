#include "duckdb/storage/table/split_column_data.hpp"

#include "duckdb/storage/table/struct_column_data.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"

namespace duckdb {

SplitColumnData::SplitColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, LogicalType type,
                                 ColumnDataType data_type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, std::move(type), data_type, parent) {
	if (data_type != ColumnDataType::CHECKPOINT_TARGET) {
		// Create the validity column
		validity = make_shared_ptr<ValidityColumnData>(block_manager, info, 0, *this);

		// Create the base column
		// We do this manually to avoid infinite recursion
		base_column = make_shared_ptr<StandardColumnData>(block_manager, info, 1, type, data_type, this);

	} else {
		// Leave empty, gets populated by 'SetBaseColumnData' and 'SetShreddedColumnData'
	}
}

idx_t SplitColumnData::GetMaxEntry() {
	return base_column->GetMaxEntry();
}

void SplitColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) {
	validity->InitializePrefetch(prefetch_state, scan_state.child_states[0], rows);
	base_column->InitializePrefetch(prefetch_state, scan_state.child_states[1], rows);

	for (idx_t i = 0; i < shredded_columns.size(); i++) {
		shredded_columns[i]->InitializePrefetch(prefetch_state, scan_state.child_states[i + 2], rows);
	}
}

void SplitColumnData::CreateScanStates(ColumnScanState &state) {
	//! Re-initialize the scan state, since there can be different shapes for every RowGroup
	state.child_states.clear();

	// Initialize validity
	state.child_states.emplace_back(state.parent);
	state.child_states[0].scan_options = state.scan_options;

	// Initialize base column
	state.child_states.emplace_back(state.parent);
	state.child_states[1].Initialize(state.context, base_column->type, state.scan_options);

	// Initialize shredded columns
	for (idx_t i = 0; i < shredded_columns.size(); i++) {
		const auto &shredded_column = shredded_columns[i];
		state.child_states.emplace_back(state.parent);
		state.child_states[i + 2].Initialize(state.context, shredded_column->type, state.scan_options);
	}
}

void SplitColumnData::InitializeScan(ColumnScanState &state) {
	SplitColumnData::CreateScanStates(state);

	state.current = nullptr;

	// Initialize validity
	validity->InitializeScan(state.child_states[0]);
	base_column->InitializeScan(state.child_states[1]);

	for (idx_t i = 0; i < shredded_columns.size(); i++) {
		shredded_columns[i]->InitializeScan(state.child_states[i + 2]);
	}
}

void SplitColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	SplitColumnData::CreateScanStates(state);

	state.current = nullptr;

	// Initialize validity
	validity->InitializeScanWithOffset(state.child_states[0], row_idx);
	base_column->InitializeScanWithOffset(state.child_states[1], row_idx);

	for (idx_t i = 0; i < shredded_columns.size(); i++) {
		shredded_columns[i]->InitializeScanWithOffset(state.child_states[i + 2], row_idx);
	}
}

idx_t SplitColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                            idx_t target_count) {
	if (shredded_columns.empty()) {
		// No shredding, just scan the validity and base columns
		const auto scan_count = validity->Scan(transaction, vector_index, state.child_states[0], result, target_count);
		base_column->Scan(transaction, vector_index, state.child_states[1], result, target_count);
		return scan_count;
	}

	// Is shredded!
	vector<LogicalType> scan_types;
	scan_types.push_back(base_column->type);
	for (const auto &shredded_column : shredded_columns) {
		scan_types.push_back(shredded_column->type);
	}

	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), scan_types, target_count);

	// Scan the base column
	base_column->Scan(transaction, vector_index, state.child_states[1], scan_chunk.data[0], target_count);
	// Scan the shredded columns
	for (idx_t i = 0; i < shredded_columns.size(); i++) {
		shredded_columns[i]->Scan(transaction, vector_index, state.child_states[i + 2], scan_chunk.data[i + 1],
		                          target_count);
	}

	// TODO: Should we pass this down/set the chunk cardinality to this?
	const auto scan_count = validity->Scan(transaction, vector_index, state.child_states[0], result, target_count);

	// Now reassemble the split vectors into the result vector
	scan_chunk.SetCardinality(scan_count);
	Reassemble(scan_chunk, result);

	return scan_count;
}

idx_t SplitColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
                                     idx_t target_count) {
	if (shredded_columns.empty()) {
		// No shredding, just scan the validity and base columns
		const auto scan_count =
		    validity->ScanCommitted(vector_index, state.child_states[0], result, allow_updates, target_count);
		base_column->ScanCommitted(vector_index, state.child_states[1], result, allow_updates, target_count);
		return scan_count;
	}

	// Is shredded!
	vector<LogicalType> scan_types;
	scan_types.push_back(base_column->type);
	for (const auto &shredded_column : shredded_columns) {
		scan_types.push_back(shredded_column->type);
	}

	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), scan_types, target_count);

	// Scan the base column
	base_column->ScanCommitted(vector_index, state.child_states[1], scan_chunk.data[0], allow_updates, target_count);
	// Scan the shredded columns
	for (idx_t i = 0; i < shredded_columns.size(); i++) {
		shredded_columns[i]->ScanCommitted(vector_index, state.child_states[i + 2], scan_chunk.data[i + 1],
		                                   allow_updates, target_count);
	}
	const auto scan_count =
	    validity->ScanCommitted(vector_index, state.child_states[0], result, allow_updates, target_count);

	// Now reassemble the split vectors into the result vector
	Reassemble(scan_chunk, result);

	return scan_count;
}

idx_t SplitColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset) {
	// TODO: Is this enough?
	return base_column->ScanCount(state.child_states[1], result, count, result_offset);
}

void SplitColumnData::Skip(ColumnScanState &state, idx_t count) {
	validity->Skip(state.child_states[0], count);
	base_column->Skip(state.child_states[1], count);

	for (idx_t i = 0; i < shredded_columns.size(); i++) {
		shredded_columns[i]->Skip(state.child_states[i + 2], count);
	}
}

void SplitColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnAppendState validity_append;
	validity->InitializeAppend(validity_append);
	state.child_appends.push_back(std::move(validity_append));

	ColumnAppendState base_append;
	base_column->InitializeAppend(base_append);
	state.child_appends.push_back(std::move(base_append));

	for (const auto &shredded_column : shredded_columns) {
		ColumnAppendState shredded_append;
		shredded_column->InitializeAppend(shredded_append);
		state.child_appends.push_back(std::move(shredded_append));
	}
}

void SplitColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t row_count) {
	if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
		Vector append_vector(vector);
		append_vector.Flatten(row_count);
		Append(stats, state, append_vector, row_count);
		return;
	}

	validity->Append(stats, state.child_appends[0], vector, row_count);

	if (shredded_columns.empty()) {
		// No shredding, just append to the base column
		base_column->Append(stats, state.child_appends[1], vector, row_count);
	} else {
		// Is shredded!
		throw NotImplementedException("SplitColumnData Append with shredding is not implemented yet.");
	}

	count += row_count;
}

void SplitColumnData::RevertAppend(row_t new_count) {
	validity->RevertAppend(new_count);
	base_column->RevertAppend(new_count);

	for (const auto &shredded_column : shredded_columns) {
		shredded_column->RevertAppend(new_count);
	}

	count = UnsafeNumericCast<idx_t>(new_count);
}

idx_t SplitColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("SPLIT Fetch");
}

void SplitColumnData::Update(TransactionData transaction, DataTable &data_table, idx_t column_index,
                             Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t row_group_start) {
	throw NotImplementedException("SPLIT Update");
}

void SplitColumnData::UpdateColumn(TransactionData transaction, DataTable &data_table,
                                   const vector<column_t> &column_path, Vector &update_vector, row_t *row_ids,
                                   idx_t update_count, idx_t depth, idx_t row_group_start) {
	throw NotImplementedException("SPLIT Update Column");
}

unique_ptr<BaseStatistics> SplitColumnData::GetUpdateStatistics() {
	return nullptr;
}

void SplitColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                               idx_t result_idx) {
	// For the base column
	state.child_states.push_back(make_uniq<ColumnFetchState>());

	// Not shredded, just fetch validity and base column
	if (shredded_columns.empty()) {
		validity->FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);
		base_column->FetchRow(transaction, *state.child_states[1], row_id, result, result_idx);
		return;
	}

	// Shredded! Add child states for shredded columns
	for (idx_t i = 0; i < shredded_columns.size(); i++) {
		state.child_states.push_back(make_uniq<ColumnFetchState>());
	}

	// TODO: Setup intermediate structure for reassembly
	DataChunk fetch_chunk;
	vector<LogicalType> fetch_types;
	fetch_types.push_back(base_column->type);
	for (const auto &shredded_column : shredded_columns) {
		fetch_types.push_back(shredded_column->type);
	}
	fetch_chunk.Initialize(Allocator::DefaultAllocator(), fetch_types, 1);
	fetch_chunk.SetCardinality(1);

	// Fetch the validity state
	validity->FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);

	// Fetch the base column
	base_column->FetchRow(transaction, *state.child_states[1], row_id, fetch_chunk.data[0], 0);

	// Fetch the shredded columns
	for (idx_t i = 0; i < shredded_columns.size(); i++) {
		shredded_columns[i]->FetchRow(transaction, *state.child_states[i + 2], row_id, fetch_chunk.data[1 + i], 0);
	}

	// TODO: Keep this vector alive
	Vector base_vector(base_column->type);
	Reassemble(fetch_chunk, base_vector);
	result.SetValue(result_idx, base_vector.GetValue(0));
}

void SplitColumnData::CommitDropColumn() {
	validity->CommitDropColumn();
	base_column->CommitDropColumn();

	for (const auto &shredded_column : shredded_columns) {
		shredded_column->CommitDropColumn();
	}
}
void SplitColumnData::SetValidityColumnData(shared_ptr<ValidityColumnData> validity_p) {
	if (validity) {
		throw InternalException("SplitColumnData::SetValidityColumnData cannot be used to overwrite existing validity");
	}
	validity_p->SetParent(this);
	validity = std::move(validity_p);
}

void SplitColumnData::SetBaseColumnData(shared_ptr<ColumnData> base_column_p) {
	if (base_column) {
		throw InternalException("SplitColumnData::SetBaseColumnData cannot be used to overwrite existing base column");
	}
	base_column_p->SetParent(this);
	base_column = std::move(base_column_p);
}

void SplitColumnData::SetShreddedColumnData(vector<shared_ptr<ColumnData>> shredded_columns_p) {
	if (!shredded_columns.empty()) {
		throw InternalException(
		    "SplitColumnData::SetShreddedColumnData cannot be used to overwrite existing shredded columns");
	}
	for (const auto &col : shredded_columns_p) {
		col->SetParent(this);
	}
	shredded_columns = std::move(shredded_columns_p);
}

namespace {

struct SplitColumnCheckpointState final : public ColumnCheckpointState {
	SplitColumnCheckpointState(const RowGroup &row_group, ColumnData &column_data,
	                           PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		// hmmmm... Initialize stats somehow
		global_stats = BaseStatistics::CreateEmpty(column_data.type).ToUnique();
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	unique_ptr<ColumnCheckpointState> base_column_state;
	vector<unique_ptr<ColumnCheckpointState>> shredded_column_states;

	shared_ptr<ColumnData> new_base_data;
	vector<shared_ptr<ColumnData>> new_shredded_data;

public:
	shared_ptr<ColumnData> CreateEmptyColumnData() override {
		auto &column = original_column.Cast<SplitColumnData>();

		return column.Create(original_column.GetBlockManager(), original_column.GetTableInfo(),
		                     original_column.column_index, original_column.type, ColumnDataType::CHECKPOINT_TARGET,
		                     nullptr);
	}

	shared_ptr<ColumnData> GetFinalResult() override {
		if (!result_column) {
			result_column = CreateEmptyColumnData();
		}

		auto &column_data = result_column->Cast<SplitColumnData>();

		auto validity_child = validity_state->GetFinalResult();
		column_data.SetValidityColumnData(shared_ptr_cast<ColumnData, ValidityColumnData>(std::move(validity_child)));

		auto base_child = base_column_state->GetFinalResult();
		column_data.SetBaseColumnData(std::move(base_child));

		vector<shared_ptr<ColumnData>> shredded_children;
		for (const auto &shredded_state : shredded_column_states) {
			shredded_children.push_back(shredded_state->GetFinalResult());
		}
		column_data.SetShreddedColumnData(std::move(shredded_children));

		return ColumnCheckpointState::GetFinalResult();
	}

	unique_ptr<BaseStatistics> GetStatistics() override {
		D_ASSERT(global_stats);
		global_stats->Merge(*validity_state->GetStatistics());
		global_stats->Merge(*base_column_state->GetStatistics());

		// Resolve the nested stats properly here
		if (!shredded_column_states.empty()) {
			vector<unique_ptr<BaseStatistics>> shredded_stats;

			for (const auto &shredded_state : shredded_column_states) {
				auto stats = shredded_state->GetStatistics();
				shredded_stats.push_back(std::move(stats));
			}

			// Call the columns CombineStats method to combine the stats
			auto &split_column_data = original_column.Cast<SplitColumnData>();
			split_column_data.CombineStats(shredded_stats, *global_stats);
		}

		return std::move(global_stats);
	}

	PersistentColumnData ToPersistentData() override {
		PersistentColumnData data(original_column.type);

		data.child_columns.push_back(validity_state->ToPersistentData());

		// Set the split types here too
		auto &split_column_data = GetResultColumn().Cast<SplitColumnData>();

		data.child_columns.push_back(base_column_state->ToPersistentData());
		data.split_types.push_back(split_column_data.base_column->type);

		for (idx_t i = 0; i < shredded_column_states.size(); i++) {
			data.child_columns.push_back(shredded_column_states[i]->ToPersistentData());
			data.split_types.push_back(split_column_data.shredded_columns[i]->type);
		}

		return data;
	}
};

} // namespace

unique_ptr<ColumnCheckpointState> SplitColumnData::CreateCheckpointState(const RowGroup &row_group,
                                                                         PartialBlockManager &partial_block_manager) {
	throw NotImplementedException("SplitColumnData CreateCheckpointState is not implemented yet.");
}

unique_ptr<ColumnCheckpointState> SplitColumnData::Checkpoint(const RowGroup &row_group, ColumnCheckpointInfo &info) {
	auto &partial_block_manager = info.GetPartialBlockManager();
	auto checkpoint_state = make_uniq<SplitColumnCheckpointState>(row_group, *this, partial_block_manager);

	auto should_shred = true;

	// Check if this column should be shredded
	if (!HasAnyChanges()) {
		should_shred = false;
	}

	// Check if this column even can be shredded
	vector<LogicalType> new_shredded_types;
	if (should_shred) {
		GetSplitTypes(new_shredded_types);

		if (new_shredded_types.empty()) {
			// Could not determine split types
			should_shred = false;
		}
	}

	if (!should_shred) {
		// No shredding required, keep the existing columns
		checkpoint_state->validity_state = validity->Checkpoint(row_group, info);
		checkpoint_state->base_column_state = base_column->Checkpoint(row_group, info);

		for (const auto &shredded_column : shredded_columns) {
			auto shredded_state = shredded_column->Checkpoint(row_group, info);
			checkpoint_state->shredded_column_states.push_back(std::move(shredded_state));
		}

		return std::move(checkpoint_state);
	}

	// Make a new base column to append into
	auto new_base_column =
	    make_shared_ptr<StandardColumnData>(block_manager, this->info, 1, base_column->type, GetDataType(), this);

	// Make new shredded columns to append into
	vector<shared_ptr<ColumnData>> new_shredded_columns;
	for (idx_t i = 0; i < new_shredded_types.size(); i++) {
		auto &new_shredded_type = new_shredded_types[i];
		auto shredded_column_data =
		    ColumnData::CreateColumn(block_manager, this->info, i + 2, new_shredded_type, GetDataType(), this);
		new_shredded_columns.push_back(std::move(shredded_column_data));
	}

	// Setup split chunk
	DataChunk split_chunk;
	vector<LogicalType> split_column_types;
	split_column_types.push_back(new_base_column->type);
	for (const auto &shredded_column : new_shredded_columns) {
		split_column_types.push_back(shredded_column->type);
	}
	split_chunk.Initialize(Allocator::DefaultAllocator(), split_column_types, STANDARD_VECTOR_SIZE);

	// Initialize append states for split columns
	vector<ColumnAppendState> split_append_states;

	ColumnAppendState base_append_state;
	new_base_column->InitializeAppend(base_append_state);
	split_append_states.push_back(std::move(base_append_state));

	for (const auto &shredded_column : new_shredded_columns) {
		ColumnAppendState split_append_state;
		shredded_column->InitializeAppend(split_append_state);
		split_append_states.push_back(std::move(split_append_state));
	}

	// Setup scan chunk
	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), {base_column->type}, STANDARD_VECTOR_SIZE);
	auto &scan_vector = scan_chunk.data[0];

	ColumnScanState scan_state(nullptr);
	InitializeScan(scan_state);

	// Now scan through the data, reassemble, and split
	idx_t total_count = count.load();
	idx_t vector_index = 0;

	for (idx_t scanned = 0; scanned < total_count; scanned += STANDARD_VECTOR_SIZE) {
		scan_chunk.Reset();

		auto to_scan = MinValue(total_count - scanned, static_cast<idx_t>(STANDARD_VECTOR_SIZE));
		ScanCommitted(vector_index++, scan_state, scan_vector, false, to_scan);

		// Verify the scan chunk
		scan_chunk.Verify();

		split_chunk.Reset();
		split_chunk.SetCardinality(to_scan);

		// Make the split
		Split(scan_vector, split_chunk);

		// The first vector is always the base column
		new_base_column->Append(*checkpoint_state->global_stats, split_append_states[0], split_chunk.data[0], to_scan);

		for (idx_t i = 0; i < new_shredded_columns.size(); i++) {
			auto &state = split_append_states[i + 1];
			auto &vec = split_chunk.data[i + 1];

			// TODO: Actually merge the stats into global stats
			// Its probably better do this here than in the `CheckpointState::GetStatistics()`
			// Actually, we need to do this here to get proper stats for shredded columns
			auto dummy_stats = BaseStatistics::CreateEmpty(new_shredded_columns[i]->type);

			new_shredded_columns[i]->Append(dummy_stats, state, vec, to_scan);
		}
	}

	// Move our new columns into the checkpoint state
	checkpoint_state->new_base_data = std::move(new_base_column);
	checkpoint_state->new_shredded_data = std::move(new_shredded_columns);

	// Now checkpoint the validity, base, and shredded columns
	checkpoint_state->validity_state = validity->Checkpoint(row_group, info);
	checkpoint_state->base_column_state = checkpoint_state->new_base_data->Checkpoint(row_group, info);

	for (const auto &shredded_column : checkpoint_state->new_shredded_data) {
		auto shredded_state = shredded_column->Checkpoint(row_group, info);
		checkpoint_state->shredded_column_states.push_back(std::move(shredded_state));
	}

	return std::move(checkpoint_state);
}

bool SplitColumnData::IsPersistent() {
	if (!validity->IsPersistent()) {
		return false;
	}
	if (!base_column->IsPersistent()) {
		return false;
	}
	for (const auto &shredded_column : shredded_columns) {
		if (!shredded_column->IsPersistent()) {
			return false;
		}
	}
	return true;
}

bool SplitColumnData::HasAnyChanges() const {
	if (validity->HasAnyChanges()) {
		return true;
	}
	if (base_column->HasAnyChanges()) {
		return true;
	}
	for (const auto &shredded_column : shredded_columns) {
		if (shredded_column->HasAnyChanges()) {
			return true;
		}
	}
	return false;
}

PersistentColumnData SplitColumnData::Serialize() {
	// TODO: Maybe make this a struct?
	PersistentColumnData persistent_data(type);
	persistent_data.child_columns.push_back(validity->Serialize());
	persistent_data.child_columns.push_back(base_column->Serialize());

	// Set split types as well
	persistent_data.split_types.push_back(base_column->type);

	for (const auto &shredded_column : shredded_columns) {
		persistent_data.split_types.push_back(shredded_column->type);
		persistent_data.child_columns.push_back(shredded_column->Serialize());
	}

	return persistent_data;
}

void SplitColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	// Initialize validity, base, and shredded columns
	validity->InitializeColumn(column_data.child_columns[0], target_stats);
	base_column->InitializeColumn(column_data.child_columns[1], target_stats);

	for (idx_t i = 2; i < column_data.child_columns.size(); i++) {
		auto &split_type = column_data.split_types[i - 1];
		shredded_columns.push_back(ColumnData::CreateColumn(block_manager, info, i, split_type, GetDataType(), this));

		// TODO: Dont do this:
		// Create dummy stats for shredded columns
		auto dummy_stats = BaseStatistics::CreateEmpty(split_type);
		shredded_columns.back()->InitializeColumn(column_data.child_columns[i], dummy_stats);
	}

	// Set the count
	count = validity->count.load();
}

void SplitColumnData::GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index, vector<idx_t> col_path,
                                           vector<ColumnSegmentInfo> &result) {
	col_path.push_back(0);
	validity->GetColumnSegmentInfo(context, row_group_index, col_path, result);

	col_path.back() = 1;
	base_column->GetColumnSegmentInfo(context, row_group_index, col_path, result);

	for (idx_t i = 0; i < shredded_columns.size(); i++) {
		col_path.back() = i + 2;
		shredded_columns[i]->GetColumnSegmentInfo(context, row_group_index, col_path, result);
	}
}

void SplitColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);

	validity->Verify(parent);
	base_column->Verify(parent);
	for (const auto &shredded_column : shredded_columns) {
		shredded_column->Verify(parent);
	}
#endif
}

} // namespace duckdb
