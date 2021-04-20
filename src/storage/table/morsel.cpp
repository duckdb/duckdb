#include "duckdb/storage/table/morsel.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

constexpr const idx_t Morsel::MORSEL_VECTOR_COUNT;
constexpr const idx_t Morsel::MORSEL_SIZE;
constexpr const idx_t Morsel::MORSEL_LAYER_COUNT;
constexpr const idx_t Morsel::MORSEL_LAYER_SIZE;

Morsel::Morsel(DatabaseInstance &db, DataTableInfo &table_info, idx_t start, idx_t count) :
    SegmentBase(start, count), db(db), table_info(table_info) {
}

Morsel::~Morsel() {}

ChunkInfo *Morsel::GetChunkInfo(idx_t vector_idx) {
	if (!version_info) {
		return nullptr;
	}
	return version_info->info[vector_idx].get();
}

idx_t Morsel::GetSelVector(Transaction &transaction, idx_t vector_idx, SelectionVector &sel_vector,
                               idx_t max_count) {
	lock_guard<mutex> lock(morsel_lock);

	auto info = GetChunkInfo(vector_idx);
	if (!info) {
		return max_count;
	}
	return info->GetSelVector(transaction, sel_vector, max_count);
}

bool Morsel::Fetch(Transaction &transaction, idx_t row) {
	D_ASSERT(row < Morsel::MORSEL_SIZE);
	lock_guard<mutex> lock(morsel_lock);

	idx_t vector_index = row / STANDARD_VECTOR_SIZE;
	auto info = GetChunkInfo(vector_index);
	if (!info) {
		return true;
	}
	return info->Fetch(transaction, row - vector_index * STANDARD_VECTOR_SIZE);
}

void Morsel::Append(Transaction &transaction, idx_t morsel_start, idx_t count, transaction_t commit_id) {
	idx_t morsel_end = morsel_start + count;
	lock_guard<mutex> lock(morsel_lock);

	// create the version_info if it doesn't exist yet
	if (!version_info) {
		version_info = make_unique<VersionNode>();
	}
	idx_t start_vector_idx = morsel_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (morsel_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t start = vector_idx == start_vector_idx ? morsel_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t end =
		    vector_idx == end_vector_idx ? morsel_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;
		if (start == 0 && end == STANDARD_VECTOR_SIZE) {
			// entire vector is encapsulated by append: append a single constant
			auto constant_info = make_unique<ChunkConstantInfo>(this->start + vector_idx * STANDARD_VECTOR_SIZE, *this);
			constant_info->insert_id = commit_id;
			constant_info->delete_id = NOT_DELETED_ID;
			version_info->info[vector_idx] = move(constant_info);
		} else {
			// part of a vector is encapsulated: append to that part
			ChunkVectorInfo *info;
			if (!version_info->info[vector_idx]) {
				// first time appending to this vector: create new info
				auto insert_info = make_unique<ChunkVectorInfo>(this->start + vector_idx * STANDARD_VECTOR_SIZE, *this);
				info = insert_info.get();
				version_info->info[vector_idx] = move(insert_info);
			} else {
				D_ASSERT(version_info->info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
				// use existing vector
				info = (ChunkVectorInfo *)version_info->info[vector_idx].get();
			}
			info->Append(start, end, commit_id);
		}
	}
}

void Morsel::CommitAppend(transaction_t commit_id, idx_t morsel_start, idx_t count) {
	D_ASSERT(version_info.get());
	idx_t morsel_end = morsel_start + count;
	lock_guard<mutex> lock(morsel_lock);

	idx_t start_vector_idx = morsel_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (morsel_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t start = vector_idx == start_vector_idx ? morsel_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t end =
		    vector_idx == end_vector_idx ? morsel_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;

		auto info = version_info->info[vector_idx].get();
		info->CommitAppend(commit_id, start, end);
	}
}

void Morsel::RevertAppend(idx_t morsel_start) {
	if (!version_info) {
		return;
	}
	idx_t start_row = morsel_start - this->start;
	idx_t start_vector_idx = (start_row + (STANDARD_VECTOR_SIZE - 1)) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx < Morsel::MORSEL_VECTOR_COUNT; vector_idx++) {
		version_info->info[vector_idx].reset();
	}
}

class VersionDeleteState {
public:
	VersionDeleteState(Morsel &info, Transaction &transaction, DataTable *table, idx_t base_row)
	    : info(info), transaction(transaction), table(table), current_info(nullptr), current_chunk(INVALID_INDEX),
	      count(0), base_row(base_row) {
	}

	Morsel &info;
	Transaction &transaction;
	DataTable *table;
	ChunkVectorInfo *current_info;
	idx_t current_chunk;
	row_t rows[STANDARD_VECTOR_SIZE];
	idx_t count;
	idx_t base_row;
	idx_t chunk_row;

public:
	void Delete(row_t row_id);
	void Flush();
};

void Morsel::Delete(Transaction &transaction, DataTable *table, Vector &row_ids, idx_t count) {
	lock_guard<mutex> lock(morsel_lock);
	VersionDeleteState del_state(*this, transaction, table, this->start);

	VectorData rdata;
	row_ids.Orrify(count, rdata);
	// obtain a write lock
	auto ids = (row_t *)rdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto ridx = rdata.sel->get_index(i);
		del_state.Delete(ids[ridx] - this->start);
	}
	del_state.Flush();
}

void VersionDeleteState::Delete(row_t row_id) {
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
			    make_unique<ChunkVectorInfo>(info.start + vector_idx * STANDARD_VECTOR_SIZE, info);
		} else if (info.version_info->info[vector_idx]->type == ChunkInfoType::CONSTANT_INFO) {
			auto &constant = (ChunkConstantInfo &)*info.version_info->info[vector_idx];
			// info exists but it's a constant info: convert to a vector info
			auto new_info = make_unique<ChunkVectorInfo>(info.start + vector_idx * STANDARD_VECTOR_SIZE, info);
			new_info->insert_id = constant.insert_id;
			for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
				new_info->inserted[i] = constant.insert_id;
			}
			info.version_info->info[vector_idx] = move(new_info);
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
	// delete in the current info
	current_info->Delete(transaction, rows, count);
	// now push the delete into the undo buffer
	transaction.PushDelete(table, current_info, rows, count, base_row + chunk_row);
	count = 0;
}

} // namespace duckdb
