#include "duckdb/storage/table/row_version_manager.hpp"

namespace duckdb {


void RowVersionManager::SetStart(idx_t start) {
	idx_t current_start = start;
	for (idx_t i = 0; i < Storage::ROW_GROUP_VECTOR_COUNT; i++) {
		if (vector_info[i]) {
			vector_info[i]->start = current_start;
		}
		current_start += STANDARD_VECTOR_SIZE;
	}
}

idx_t RowVersionManager::GetCommittedDeletedCount(idx_t count) {
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

idx_t RowVersionManager::GetSelVector(TransactionData transaction, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count) {
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

void RowVersionManager::AppendVersionInfo(TransactionData transaction, idx_t count, idx_t row_group_start, idx_t row_group_end, idx_t start) {
	lock_guard<mutex> lock(version_lock);
	idx_t start_vector_idx = row_group_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (row_group_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t vector_start = vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t vector_end =
				vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE
											 : STANDARD_VECTOR_SIZE;
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
			} else {
				D_ASSERT(vector_info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
				// use existing vector
				new_info = &vector_info[vector_idx]->Cast<ChunkVectorInfo>();
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
		idx_t start = vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t end =
		    vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;

		auto info = vector_info[vector_idx].get();
		info->CommitAppend(commit_id, start, end);
	}
}

void RowVersionManager::RevertAppend(idx_t start_row) {
	lock_guard<mutex> lock(version_lock);
	idx_t start_vector_idx = (start_row + (STANDARD_VECTOR_SIZE - 1)) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx < Storage::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		vector_info[vector_idx].reset();
	}
}

ChunkVectorInfo &RowVersionManager::GetVectorInfo(idx_t start_row, idx_t vector_idx) {
	lock_guard<mutex> lock(version_lock);
	if (!vector_info[vector_idx]) {
		// no info yet: create it
		vector_info[vector_idx] =
			make_uniq<ChunkVectorInfo>(start_row + vector_idx * STANDARD_VECTOR_SIZE);
	} else if (vector_info[vector_idx]->type == ChunkInfoType::CONSTANT_INFO) {
		auto &constant = vector_info[vector_idx]->Cast<ChunkConstantInfo>();
		// info exists but it's a constant info: convert to a vector info
		auto new_info = make_uniq<ChunkVectorInfo>(start_row + vector_idx * STANDARD_VECTOR_SIZE);
		new_info->insert_id = constant.insert_id.load();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			new_info->inserted[i] = constant.insert_id.load();
		}
		vector_info[vector_idx] = std::move(new_info);
	}
	D_ASSERT(vector_info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
	return vector_info[vector_idx]->Cast<ChunkVectorInfo>();
}

MetaBlockPointer RowVersionManager::Checkpoint(MetadataManager &manager) {
	// first count how many ChunkInfo's we need to deserialize
	idx_t chunk_info_count = 0;
	for (idx_t vector_idx = 0; vector_idx < Storage::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		auto chunk_info = vector_info[vector_idx].get();
		if (!chunk_info) {
			continue;
		}
		chunk_info_count++;
	}
	if (chunk_info_count == 0) {
		return MetaBlockPointer();
	}

	MetadataWriter writer(manager);
	auto delete_location = writer.GetMetaBlockPointer();
	// now serialize the actual version information
	writer.Write<idx_t>(chunk_info_count);
	for (idx_t vector_idx = 0; vector_idx < Storage::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		auto chunk_info = vector_info[vector_idx].get();
		if (!chunk_info) {
			continue;
		}
		writer.Write<idx_t>(vector_idx);
		chunk_info->Serialize(writer);
	}
	writer.Flush();
	return delete_location;
}

shared_ptr<RowVersionManager> RowVersionManager::Deserialize(MetaBlockPointer delete_pointer, MetadataManager &manager) {
	if (!delete_pointer.IsValid()) {
		return nullptr;
	}
	MetadataReader source(manager, delete_pointer);
	auto version_info = make_shared<RowVersionManager>();
	auto chunk_count = source.Read<idx_t>();
	D_ASSERT(chunk_count > 0);
	for (idx_t i = 0; i < chunk_count; i++) {
		idx_t vector_index = source.Read<idx_t>();
		if (vector_index >= Storage::ROW_GROUP_VECTOR_COUNT) {
			throw Exception("In DeserializeDeletes, vector_index is out of range for the row group. Corrupted file?");
		}
		version_info->vector_info[vector_index] = ChunkInfo::Deserialize(source);
	}
	return version_info;
}

}
