#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

constexpr const idx_t RowGroup::ROW_GROUP_VECTOR_COUNT;
constexpr const idx_t RowGroup::ROW_GROUP_SIZE;

RowGroup::RowGroup(RowGroupCollection &collection, idx_t start, idx_t count)
    : SegmentBase<RowGroup>(start, count), collection(collection) {
}

RowGroup::RowGroup(RowGroupCollection &collection, RowGroupPointer &&pointer)
    : SegmentBase<RowGroup>(pointer.row_start, pointer.tuple_count), collection(collection) {
	// deserialize the columns
	if (pointer.data_pointers.size() != collection.GetTypes().size()) {
		throw IOException("Row group column count is unaligned with table column count. Corrupt file?");
	}
	this->version_info = std::move(pointer.versions);
}

RowGroup::RowGroup(RowGroup &row_group, RowGroupCollection &collection, idx_t start)
    : SegmentBase<RowGroup>(start, row_group.count.load()), collection(collection),
      version_info(std::move(row_group.version_info)) {
	if (version_info) {
		version_info->SetStart(start);
	}
}

void VersionNode::SetStart(idx_t start) {
	idx_t current_start = start;
	for (idx_t i = 0; i < RowGroup::ROW_GROUP_VECTOR_COUNT; i++) {
		if (info[i]) {
			info[i]->start = current_start;
		}
		current_start += STANDARD_VECTOR_SIZE;
	}
}

RowGroup::~RowGroup() {
}

DatabaseInstance &RowGroup::GetDatabase() {
	return collection.GetDatabase();
}

BlockManager &RowGroup::GetBlockManager() {
	return collection.GetBlockManager();
}
DataTableInfo &RowGroup::GetTableInfo() {
	return collection.GetTableInfo();
}

void ColumnScanState::Initialize(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VALIDITY) {
		// validity - nothing to initialize
		return;
	}
	if (type.InternalType() == PhysicalType::STRUCT) {
		// validity + struct children
		auto &struct_children = StructType::GetChildTypes(type);
		child_states.resize(struct_children.size() + 1);
		for (idx_t i = 0; i < struct_children.size(); i++) {
			child_states[i + 1].Initialize(struct_children[i].second);
		}
	} else if (type.InternalType() == PhysicalType::LIST) {
		// validity + list child
		child_states.resize(2);
		child_states[1].Initialize(ListType::GetChildType(type));
	} else {
		// validity
		child_states.resize(1);
	}
}

void CollectionScanState::Initialize(const vector<LogicalType> &types) {
	auto &column_ids = GetColumnIds();
	column_scans = unique_ptr<ColumnScanState[]>(new ColumnScanState[column_ids.size()]);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i] == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		column_scans[i].Initialize(types[column_ids[i]]);
	}
}

void RowGroup::InitializeScanWithOffset(CollectionScanState &state, idx_t vector_offset) {
	state.row_group = this;
	state.vector_index = vector_offset;
	state.max_row_group_row =
	    this->start > state.max_row ? 0 : MinValue<idx_t>(this->count, state.max_row - this->start);
}

void RowGroup::InitializeScan(CollectionScanState &state) {
	state.row_group = this;
	state.vector_index = 0;
	state.max_row_group_row =
	    this->start > state.max_row ? 0 : MinValue<idx_t>(this->count, state.max_row - this->start);
}

ChunkInfo *RowGroup::GetChunkInfo(idx_t vector_idx) {
	if (!version_info) {
		return nullptr;
	}
	return version_info->info[vector_idx].get();
}

idx_t RowGroup::GetSelVector(TransactionData transaction, idx_t vector_idx, SelectionVector &sel_vector,
                             idx_t max_count) {
	lock_guard<mutex> lock(row_group_lock);

	auto info = GetChunkInfo(vector_idx);
	if (!info) {
		return max_count;
	}
	return info->GetSelVector(transaction, sel_vector, max_count);
}

idx_t RowGroup::GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
                                      SelectionVector &sel_vector, idx_t max_count) {
	lock_guard<mutex> lock(row_group_lock);

	auto info = GetChunkInfo(vector_idx);
	if (!info) {
		return max_count;
	}
	return info->GetCommittedSelVector(start_time, transaction_id, sel_vector, max_count);
}

bool RowGroup::Fetch(TransactionData transaction, idx_t row) {
	D_ASSERT(row < this->count);
	lock_guard<mutex> lock(row_group_lock);

	idx_t vector_index = row / STANDARD_VECTOR_SIZE;
	auto info = GetChunkInfo(vector_index);
	if (!info) {
		return true;
	}
	return info->Fetch(transaction, row - vector_index * STANDARD_VECTOR_SIZE);
}

void RowGroup::AppendVersionInfo(TransactionData transaction, idx_t count) {
	idx_t row_group_start = this->count.load();
	idx_t row_group_end = row_group_start + count;
	if (row_group_end > RowGroup::ROW_GROUP_SIZE) {
		row_group_end = RowGroup::ROW_GROUP_SIZE;
	}
	lock_guard<mutex> lock(row_group_lock);

	// create the version_info if it doesn't exist yet
	if (!version_info) {
		version_info = make_unique<VersionNode>();
	}
	idx_t start_vector_idx = row_group_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (row_group_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t start = vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t end =
		    vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;
		if (start == 0 && end == STANDARD_VECTOR_SIZE) {
			// entire vector is encapsulated by append: append a single constant
			auto constant_info = make_unique<ChunkConstantInfo>(this->start + vector_idx * STANDARD_VECTOR_SIZE);
			constant_info->insert_id = transaction.transaction_id;
			constant_info->delete_id = NOT_DELETED_ID;
			version_info->info[vector_idx] = std::move(constant_info);
		} else {
			// part of a vector is encapsulated: append to that part
			ChunkVectorInfo *info;
			if (!version_info->info[vector_idx]) {
				// first time appending to this vector: create new info
				auto insert_info = make_unique<ChunkVectorInfo>(this->start + vector_idx * STANDARD_VECTOR_SIZE);
				info = insert_info.get();
				version_info->info[vector_idx] = std::move(insert_info);
			} else {
				D_ASSERT(version_info->info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
				// use existing vector
				info = (ChunkVectorInfo *)version_info->info[vector_idx].get();
			}
			info->Append(start, end, transaction.transaction_id);
		}
	}
	this->count = row_group_end;
}

void RowGroup::CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count) {
	D_ASSERT(version_info.get());
	idx_t row_group_end = row_group_start + count;
	lock_guard<mutex> lock(row_group_lock);

	idx_t start_vector_idx = row_group_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (row_group_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t start = vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t end =
		    vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;

		auto info = version_info->info[vector_idx].get();
		info->CommitAppend(commit_id, start, end);
	}
}

void RowGroup::RevertAppend(idx_t row_group_start) {
	if (!version_info) {
		return;
	}
	idx_t start_row = row_group_start - this->start;
	idx_t start_vector_idx = (start_row + (STANDARD_VECTOR_SIZE - 1)) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx < RowGroup::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		version_info->info[vector_idx].reset();
	}
	this->count = MinValue<idx_t>(row_group_start - this->start, this->count);
}

void RowGroup::InitializeAppend(RowGroupAppendState &append_state) {
	append_state.row_group = this;
	append_state.offset_in_row_group = this->count;
}

void RowGroup::Append(RowGroupAppendState &state, DataChunk &chunk, idx_t append_count) {
	state.offset_in_row_group += append_count;
}

void RowGroup::CheckpointDeletes(VersionNode *versions, Serializer &serializer) {
	if (!versions) {
		// no version information: write nothing
		serializer.Write<idx_t>(0);
		return;
	}
	// first count how many ChunkInfo's we need to deserialize
	idx_t chunk_info_count = 0;
	for (idx_t vector_idx = 0; vector_idx < RowGroup::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		auto chunk_info = versions->info[vector_idx].get();
		if (!chunk_info) {
			continue;
		}
		chunk_info_count++;
	}
	// now serialize the actual version information
	serializer.Write<idx_t>(chunk_info_count);
	for (idx_t vector_idx = 0; vector_idx < RowGroup::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		auto chunk_info = versions->info[vector_idx].get();
		if (!chunk_info) {
			continue;
		}
		serializer.Write<idx_t>(vector_idx);
		chunk_info->Serialize(serializer);
	}
}

shared_ptr<VersionNode> RowGroup::DeserializeDeletes(Deserializer &source) {
	auto chunk_count = source.Read<idx_t>();
	if (chunk_count == 0) {
		// no deletes
		return nullptr;
	}
	auto version_info = make_shared<VersionNode>();
	for (idx_t i = 0; i < chunk_count; i++) {
		idx_t vector_index = source.Read<idx_t>();
		if (vector_index >= RowGroup::ROW_GROUP_VECTOR_COUNT) {
			throw Exception("In DeserializeDeletes, vector_index is out of range for the row group. Corrupted file?");
		}
		version_info->info[vector_index] = ChunkInfo::Deserialize(source);
	}
	return version_info;
}

void RowGroup::Serialize(RowGroupPointer &pointer, Serializer &main_serializer) {
	FieldWriter writer(main_serializer);
	writer.WriteField<uint64_t>(pointer.row_start);
	writer.WriteField<uint64_t>(pointer.tuple_count);
	auto &serializer = writer.GetSerializer();
	for (auto &data_pointer : pointer.data_pointers) {
		serializer.Write<block_id_t>(data_pointer.block_id);
		serializer.Write<uint64_t>(data_pointer.offset);
	}
	CheckpointDeletes(pointer.versions.get(), serializer);
	writer.Finalize();
}

RowGroupPointer RowGroup::Deserialize(Deserializer &main_source, const vector<LogicalType> &columns) {
	RowGroupPointer result;

	FieldReader reader(main_source);
	result.row_start = reader.ReadRequired<uint64_t>();
	result.tuple_count = reader.ReadRequired<uint64_t>();

	auto physical_columns = columns.size();
	result.data_pointers.reserve(physical_columns);

	auto &source = reader.GetSource();
	for (idx_t i = 0; i < columns.size(); i++) {
		BlockPointer pointer;
		pointer.block_id = source.Read<block_id_t>();
		pointer.offset = source.Read<uint64_t>();
		result.data_pointers.push_back(pointer);
	}
	result.versions = DeserializeDeletes(source);

	reader.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// Version Delete Information
//===--------------------------------------------------------------------===//
class VersionDeleteState {
public:
	VersionDeleteState(RowGroup &info, TransactionData transaction, DataTable *table, idx_t base_row)
	    : info(info), transaction(transaction), table(table), current_info(nullptr),
	      current_chunk(DConstants::INVALID_INDEX), count(0), base_row(base_row), delete_count(0) {
	}

	RowGroup &info;
	TransactionData transaction;
	DataTable *table;
	ChunkVectorInfo *current_info;
	idx_t current_chunk;
	row_t rows[STANDARD_VECTOR_SIZE];
	idx_t count;
	idx_t base_row;
	idx_t chunk_row;
	idx_t delete_count;

public:
	void Delete(row_t row_id);
	void Flush();
};

idx_t RowGroup::Delete(TransactionData transaction, DataTable *table, row_t *ids, idx_t count) {
	lock_guard<mutex> lock(row_group_lock);
	VersionDeleteState del_state(*this, transaction, table, this->start);

	// obtain a write lock
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(ids[i] >= 0);
		D_ASSERT(idx_t(ids[i]) >= this->start && idx_t(ids[i]) < this->start + this->count);
		del_state.Delete(ids[i] - this->start);
	}
	del_state.Flush();
	return del_state.delete_count;
}

void VersionDeleteState::Delete(row_t row_id) {
	D_ASSERT(row_id >= 0);
	idx_t vector_idx = row_id / STANDARD_VECTOR_SIZE;
	idx_t idx_in_vector = row_id - vector_idx * STANDARD_VECTOR_SIZE;
	if (current_chunk != vector_idx) {
		Flush();

		if (!info.version_info) {
			info.version_info = make_unique<VersionNode>();
		}

		if (!info.version_info->info[vector_idx]) {
			// no info yet: create it
			info.version_info->info[vector_idx] =
			    make_unique<ChunkVectorInfo>(info.start + vector_idx * STANDARD_VECTOR_SIZE);
		} else if (info.version_info->info[vector_idx]->type == ChunkInfoType::CONSTANT_INFO) {
			auto &constant = (ChunkConstantInfo &)*info.version_info->info[vector_idx];
			// info exists but it's a constant info: convert to a vector info
			auto new_info = make_unique<ChunkVectorInfo>(info.start + vector_idx * STANDARD_VECTOR_SIZE);
			new_info->insert_id = constant.insert_id.load();
			for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
				new_info->inserted[i] = constant.insert_id.load();
			}
			info.version_info->info[vector_idx] = std::move(new_info);
		}
		D_ASSERT(info.version_info->info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
		current_info = (ChunkVectorInfo *)info.version_info->info[vector_idx].get();
		current_chunk = vector_idx;
		chunk_row = vector_idx * STANDARD_VECTOR_SIZE;
	}
	rows[count++] = idx_in_vector;
}

void VersionDeleteState::Flush() {
	if (count == 0) {
		return;
	}
	// it is possible for delete statements to delete the same tuple multiple times when combined with a USING clause
	// in the current_info->Delete, we check which tuples are actually deleted (excluding duplicate deletions)
	// this is returned in the actual_delete_count
	auto actual_delete_count = current_info->Delete(transaction.transaction_id, rows, count);
	delete_count += actual_delete_count;
	if (transaction.transaction && actual_delete_count > 0) {
		// now push the delete into the undo buffer, but only if any deletes were actually performed
		transaction.transaction->PushDelete(table, current_info, rows, actual_delete_count, base_row + chunk_row);
	}
	count = 0;
}

} // namespace duckdb
