#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/transaction/transaction_data.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

RowVersionManager::RowVersionManager(idx_t start) : start(start), has_changes(false) {
}

void RowVersionManager::SetStart(idx_t new_start) {
	lock_guard<mutex> l(version_lock);
	this->start = new_start;
	idx_t current_start = start;
	for (idx_t i = 0; i < Storage::ROW_GROUP_VECTOR_COUNT; i++) {
		if (vector_info[i]) {
			vector_info[i]->start = current_start;
		}
		current_start += STANDARD_VECTOR_SIZE;
	}
}

idx_t RowVersionManager::GetCommittedDeletedCount(idx_t count) {
	lock_guard<mutex> l(version_lock);
	idx_t deleted_count = 0;
	for (idx_t r = 0, i = 0; r < count; r += STANDARD_VECTOR_SIZE, i++) {
		if (!vector_info[i]) {
			continue;
		}
		idx_t max_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, count - r);
		if (max_count == 0) {
			break;
		}
		deleted_count += vector_info[i]->GetCommittedDeletedCount(max_count);
	}
	return deleted_count;
}

optional_ptr<ChunkInfo> RowVersionManager::GetChunkInfo(idx_t vector_idx) {
	return vector_info[vector_idx].get();
}

idx_t RowVersionManager::GetSelVector(TransactionData transaction, idx_t vector_idx, SelectionVector &sel_vector,
                                      idx_t max_count) {
	lock_guard<mutex> l(version_lock);
	auto chunk_info = GetChunkInfo(vector_idx);
	if (!chunk_info) {
		return max_count;
	}
	return chunk_info->GetSelVector(transaction, sel_vector, max_count);
}

idx_t RowVersionManager::GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
                                               SelectionVector &sel_vector, idx_t max_count) {
	lock_guard<mutex> l(version_lock);
	auto info = GetChunkInfo(vector_idx);
	if (!info) {
		return max_count;
	}
	return info->GetCommittedSelVector(start_time, transaction_id, sel_vector, max_count);
}

bool RowVersionManager::Fetch(TransactionData transaction, idx_t row) {
	lock_guard<mutex> lock(version_lock);
	idx_t vector_index = row / STANDARD_VECTOR_SIZE;
	auto info = GetChunkInfo(vector_index);
	if (!info) {
		return true;
	}
	return info->Fetch(transaction, row - vector_index * STANDARD_VECTOR_SIZE);
}

void RowVersionManager::AppendVersionInfo(TransactionData transaction, idx_t count, idx_t row_group_start,
                                          idx_t row_group_end) {
	lock_guard<mutex> lock(version_lock);
	has_changes = true;
	idx_t start_vector_idx = row_group_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (row_group_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t vector_start =
		    vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t vector_end =
		    vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;
		if (vector_start == 0 && vector_end == STANDARD_VECTOR_SIZE) {
			// entire vector is encapsulated by append: append a single constant
			auto constant_info = make_uniq<ChunkConstantInfo>(start + vector_idx * STANDARD_VECTOR_SIZE);
			constant_info->insert_id = transaction.transaction_id;
			constant_info->delete_id = NOT_DELETED_ID;
			vector_info[vector_idx] = std::move(constant_info);
		} else {
			// part of a vector is encapsulated: append to that part
			optional_ptr<ChunkVectorInfo> new_info;
			if (!vector_info[vector_idx]) {
				// first time appending to this vector: create new info
				auto insert_info = make_uniq<ChunkVectorInfo>(start + vector_idx * STANDARD_VECTOR_SIZE);
				new_info = insert_info.get();
				vector_info[vector_idx] = std::move(insert_info);
			} else if (vector_info[vector_idx]->type == ChunkInfoType::VECTOR_INFO) {
				// use existing vector
				new_info = &vector_info[vector_idx]->Cast<ChunkVectorInfo>();
			} else {
				throw InternalException("Error in RowVersionManager::AppendVersionInfo - expected either a "
				                        "ChunkVectorInfo or no version info");
			}
			new_info->Append(vector_start, vector_end, transaction.transaction_id);
		}
	}
}

void RowVersionManager::CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count) {
	idx_t row_group_end = row_group_start + count;

	lock_guard<mutex> lock(version_lock);
	idx_t start_vector_idx = row_group_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (row_group_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t vstart = vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t vend =
		    vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;

		auto info = vector_info[vector_idx].get();
		info->CommitAppend(commit_id, vstart, vend);
	}
}

void RowVersionManager::RevertAppend(idx_t start_row) {
	lock_guard<mutex> lock(version_lock);
	idx_t start_vector_idx = (start_row + (STANDARD_VECTOR_SIZE - 1)) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx < Storage::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		vector_info[vector_idx].reset();
	}
}

ChunkVectorInfo &RowVersionManager::GetVectorInfo(idx_t vector_idx) {
	if (!vector_info[vector_idx]) {
		// no info yet: create it
		vector_info[vector_idx] = make_uniq<ChunkVectorInfo>(start + vector_idx * STANDARD_VECTOR_SIZE);
	} else if (vector_info[vector_idx]->type == ChunkInfoType::CONSTANT_INFO) {
		auto &constant = vector_info[vector_idx]->Cast<ChunkConstantInfo>();
		// info exists but it's a constant info: convert to a vector info
		auto new_info = make_uniq<ChunkVectorInfo>(start + vector_idx * STANDARD_VECTOR_SIZE);
		new_info->insert_id = constant.insert_id;
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			new_info->inserted[i] = constant.insert_id;
		}
		vector_info[vector_idx] = std::move(new_info);
	}
	D_ASSERT(vector_info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
	return vector_info[vector_idx]->Cast<ChunkVectorInfo>();
}

idx_t RowVersionManager::DeleteRows(idx_t vector_idx, transaction_t transaction_id, row_t rows[], idx_t count) {
	lock_guard<mutex> lock(version_lock);
	has_changes = true;
	return GetVectorInfo(vector_idx).Delete(transaction_id, rows, count);
}

void RowVersionManager::CommitDelete(idx_t vector_idx, transaction_t commit_id, row_t rows[], idx_t count) {
	lock_guard<mutex> lock(version_lock);
	has_changes = true;
	GetVectorInfo(vector_idx).CommitDelete(commit_id, rows, count);
}

vector<MetaBlockPointer> RowVersionManager::Checkpoint(MetadataManager &manager) {
	if (!has_changes && !storage_pointers.empty()) {
		// the row version manager already exists on disk and no changes were made
		// we can write the current pointer as-is
		// ensure the blocks we are pointing to are not marked as free
		manager.ClearModifiedBlocks(storage_pointers);
		// return the root pointer
		return storage_pointers;
	}
	// first count how many ChunkInfo's we need to deserialize
	vector<pair<idx_t, reference<ChunkInfo>>> to_serialize;
	for (idx_t vector_idx = 0; vector_idx < Storage::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		auto chunk_info = vector_info[vector_idx].get();
		if (!chunk_info) {
			continue;
		}
		if (!chunk_info->HasDeletes()) {
			continue;
		}
		to_serialize.emplace_back(vector_idx, *chunk_info);
	}
	if (to_serialize.empty()) {
		return vector<MetaBlockPointer>();
	}

	storage_pointers.clear();

	MetadataWriter writer(manager, &storage_pointers);
	// now serialize the actual version information
	writer.Write<idx_t>(to_serialize.size());
	for (auto &entry : to_serialize) {
		auto &vector_idx = entry.first;
		auto &chunk_info = entry.second.get();
		writer.Write<idx_t>(vector_idx);
		chunk_info.Write(writer);
	}
	writer.Flush();

	has_changes = false;
	return storage_pointers;
}

shared_ptr<RowVersionManager> RowVersionManager::Deserialize(MetaBlockPointer delete_pointer, MetadataManager &manager,
                                                             idx_t start) {
	if (!delete_pointer.IsValid()) {
		return nullptr;
	}
	auto version_info = make_shared<RowVersionManager>(start);
	MetadataReader source(manager, delete_pointer, &version_info->storage_pointers);
	auto chunk_count = source.Read<idx_t>();
	D_ASSERT(chunk_count > 0);
	for (idx_t i = 0; i < chunk_count; i++) {
		idx_t vector_index = source.Read<idx_t>();
		if (vector_index >= Storage::ROW_GROUP_VECTOR_COUNT) {
			throw Exception("In DeserializeDeletes, vector_index is out of range for the row group. Corrupted file?");
		}
		version_info->vector_info[vector_index] = ChunkInfo::Read(source);
	}
	version_info->has_changes = false;
	return version_info;
}

} // namespace duckdb
