#include "storage/uncompressed_segment.hpp"

using namespace duckdb;
using namespace std;

UncompressedSegment::UncompressedSegment(ColumnData &column_data, BufferManager &manager, TypeId type) :
	column_data(column_data), manager(manager), type(type), block_id(INVALID_BLOCK), max_vector_count(0), tuple_count(0), versions(nullptr) {
}

static void CheckForConflicts(UpdateInfo *info, Transaction &transaction, Vector &update, row_t *ids, row_t offset, UpdateInfo *& node) {
	if (info->version_number == transaction.transaction_id) {
		// this UpdateInfo belongs to the current transaction, set it in the node
		node = info;
	} else if (info->version_number > transaction.start_time) {
		// potential conflict, check that tuple ids do not conflict
		// as both ids and info->tuples are sorted, this is similar to a merge join
		index_t i = 0, j = 0;
		while(true) {
			auto id = ids[i] - offset;
			if (id == info->tuples[j]) {
				throw TransactionException("Conflict on update!");
			} else if (id < info->tuples[j]) {
				// id < the current tuple in info, move to next id
				i++;
				if (i == update.count) {
					break;
				}
			} else {
				// id > the current tuple, move to next tuple in info
				j++;
				if (j == info->N) {
					break;
				}
			}
		}
	}
	if (info->next) {
		CheckForConflicts(info->next, transaction, update, ids, offset, node);
	}
}

void UncompressedSegment::Update(SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids, row_t offset) {
	// obtain an exclusive lock
	auto write_lock = lock.GetExclusiveLock();

	assert(!update.sel_vector);
#ifdef DEBUG
	// verify that the ids are sorted and there are no duplicates
	for(index_t i = 1; i < update.count; i++) {
		assert(ids[i] > ids[i - 1]);
	}
#endif

	// create the versions for this segment, if there are none yet
	if (!versions) {
		this->versions = unique_ptr<UpdateInfo*[]>(new UpdateInfo*[max_vector_count]);
		for(index_t i = 0; i < max_vector_count; i++) {
			this->versions[i] = nullptr;
		}
	}

	// get the vector index based on the first id
	// we assert that all updates must be part of the same vector
	auto first_id = update.sel_vector ? ids[update.sel_vector[0]] : ids[0];
	index_t vector_index = (first_id - offset) / STANDARD_VECTOR_SIZE;
	index_t vector_offset = offset + vector_index * STANDARD_VECTOR_SIZE;

	assert(first_id >= offset);
	assert(vector_index < max_vector_count);

	// first check the version chain
	UpdateInfo *node = nullptr;
	if (versions[vector_index]) {
		// there is already a version here, check if there are any conflicts and search for the node that belongs to this transaction in the version chain
		CheckForConflicts(versions[vector_index], transaction, update, ids, vector_offset, node);
	}
	Update(stats, transaction, update, ids, vector_index, vector_offset, node);
}

UpdateInfo *UncompressedSegment::CreateUpdateInfo(Transaction &transaction, row_t *ids, index_t count, index_t vector_index, index_t vector_offset, index_t type_size) {
	auto node = transaction.CreateUpdateInfo(type_size, STANDARD_VECTOR_SIZE);
	node->segment = this;
	node->vector_index = vector_index;
	node->prev = nullptr;
	node->next = versions[vector_index];
	if (node->next) {
		node->next->prev = node;
	}
	versions[vector_index] = node;

	// set up the tuple ids
	node->N = count;
	for(index_t i = 0; i < count; i++) {
		assert((index_t) ids[i] >= vector_offset && (index_t) ids[i] < vector_offset + STANDARD_VECTOR_SIZE);
		node->tuples[i] = ids[i] - vector_offset;
	};
	return node;
}


void UncompressedSegment::CleanupUpdate(UpdateInfo *info) {
	if (info->prev) {
		// there is a prev info: remove from the chain
		auto prev = info->prev;
		prev->next = info->next;
		if (prev->next) {
			prev->next->prev = prev;
		}
	} else {
		// there is no prev info: remove from base segment
		info->segment->versions[info->vector_index] = info->next;
		if (info->next) {
			info->next->prev = nullptr;
		}
	}
}
