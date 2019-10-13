#include "transaction/undo_buffer.hpp"

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog_set.hpp"
#include "common/exception.hpp"
#include "storage/data_table.hpp"
#include "storage/table/version_chunk.hpp"
#include "storage/write_ahead_log.hpp"
#include "transaction/cleanup_state.hpp"
#include "transaction/commit_state.hpp"
#include "transaction/rollback_state.hpp"
#include "transaction/version_info.hpp"

#include <unordered_map>

using namespace duckdb;
using namespace std;

constexpr uint32_t DEFAULT_UNDO_CHUNK_SIZE = 4096 * 3;
constexpr uint32_t UNDO_ENTRY_HEADER_SIZE = sizeof(UndoFlags) + sizeof(uint32_t);

UndoBuffer::UndoBuffer() {
	head = make_unique<UndoChunk>(0);
	tail = head.get();
}

UndoChunk::UndoChunk(index_t size) : current_position(0), maximum_size(size), prev(nullptr) {
	if (size > 0) {
		data = unique_ptr<data_t[]>(new data_t[maximum_size]);
	}
}
UndoChunk::~UndoChunk() {
	if (next) {
		auto current_next = move(next);
		while (current_next) {
			current_next = move(current_next->next);
		}
	}
}

data_ptr_t UndoChunk::WriteEntry(UndoFlags type, uint32_t len) {
	*((UndoFlags *)(data.get() + current_position)) = type;
	current_position += sizeof(UndoFlags);
	*((uint32_t *)(data.get() + current_position)) = len;
	current_position += sizeof(uint32_t);

	data_ptr_t result = data.get() + current_position;
	current_position += len;
	return result;
}

data_ptr_t UndoBuffer::CreateEntry(UndoFlags type, index_t len) {
	assert(len <= std::numeric_limits<uint32_t>::max());
	index_t needed_space = len + UNDO_ENTRY_HEADER_SIZE;
	if (head->current_position + needed_space >= head->maximum_size) {
		auto new_chunk =
		    make_unique<UndoChunk>(needed_space > DEFAULT_UNDO_CHUNK_SIZE ? needed_space : DEFAULT_UNDO_CHUNK_SIZE);
		head->prev = new_chunk.get();
		new_chunk->next = move(head);
		head = move(new_chunk);
	}
	return head->WriteEntry(type, len);
}

template <class T> void UndoBuffer::IterateEntries(T &&callback) {
	// iterate in insertion order: start with the tail
	auto current = tail;
	while (current) {
		data_ptr_t start = current->data.get();
		data_ptr_t end = start + current->current_position;
		while (start < end) {
			UndoFlags type = *((UndoFlags *)start);
			start += sizeof(UndoFlags);
			uint32_t len = *((uint32_t *)start);
			start += sizeof(uint32_t);
			callback(type, start);
			start += len;
		}
		current = current->prev;
	}
}

template <class T> void UndoBuffer::ReverseIterateEntries(T &&callback) {
	// iterate in reverse insertion order: start with the head
	auto current = head.get();
	while (current) {
		data_ptr_t start = current->data.get();
		data_ptr_t end = start + current->current_position;
		// create a vector with all nodes in this chunk
		vector<pair<UndoFlags, data_ptr_t>> nodes;
		while (start < end) {
			UndoFlags type = *((UndoFlags *)start);
			start += sizeof(UndoFlags);
			uint32_t len = *((uint32_t *)start);
			start += sizeof(uint32_t);
			nodes.push_back(make_pair(type, start));
			start += len;
		}
		// iterate over it in reverse order
		for (index_t i = nodes.size(); i > 0; i--) {
			callback(nodes[i - 1].first, nodes[i - 1].second);
		}
		current = current->next.get();
	}
}

bool UndoBuffer::ChangesMade() {
	return head->maximum_size > 0;
}

void UndoBuffer::Cleanup() {
	// garbage collect everything in the Undo Chunk
	// this should only happen if
	//  (1) the transaction this UndoBuffer belongs to has successfully
	//  committed
	//      (on Rollback the Rollback() function should be called, that clears
	//      the chunks)
	//  (2) there is no active transaction with start_id < commit_id of this
	//  transaction
	CleanupState state;
	IterateEntries([&](UndoFlags type, data_ptr_t data) { state.CleanupEntry(type, data); });
}

void UndoBuffer::Commit(WriteAheadLog *log, transaction_t commit_id) {
	if (log) {
		CommitState<true> state(commit_id, log);
		// commit WITH write ahead log
		IterateEntries([&](UndoFlags type, data_ptr_t data) { state.CommitEntry(type, data); });
		// final flush after writing
		state.Flush(UndoFlags::EMPTY_ENTRY);
	} else {
		CommitState<false> state(commit_id);
		// comit WITHOUT write ahead log
		IterateEntries([&](UndoFlags type, data_ptr_t data) { state.CommitEntry(type, data); });
	}
}

void UndoBuffer::Rollback() {
	// rollback needs to be performed in reverse
	RollbackState state;
	ReverseIterateEntries([&](UndoFlags type, data_ptr_t data) { state.RollbackEntry(type, data); });
}
