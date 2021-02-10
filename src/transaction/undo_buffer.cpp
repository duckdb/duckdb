#include "duckdb/transaction/undo_buffer.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/transaction/cleanup_state.hpp"
#include "duckdb/transaction/commit_state.hpp"
#include "duckdb/transaction/rollback_state.hpp"
#include "duckdb/common/pair.hpp"

#include <unordered_map>

namespace duckdb {
constexpr uint32_t DEFAULT_UNDO_CHUNK_SIZE = 4096 * 3;
constexpr uint32_t UNDO_ENTRY_HEADER_SIZE = sizeof(UndoFlags) + sizeof(uint32_t);

static idx_t AlignLength(idx_t len) {
	return (len + 7) / 8 * 8;
}

UndoBuffer::UndoBuffer() {
	head = make_unique<UndoChunk>(0);
	tail = head.get();
}

UndoChunk::UndoChunk(idx_t size) : current_position(0), maximum_size(size), prev(nullptr) {
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
	len = AlignLength(len);
	D_ASSERT(sizeof(UndoFlags) + sizeof(len) == 8);
	Store<UndoFlags>(type, data.get() + current_position);
	current_position += sizeof(UndoFlags);
	Store<uint32_t>(len, data.get() + current_position);
	current_position += sizeof(uint32_t);

	data_ptr_t result = data.get() + current_position;
	current_position += len;
	return result;
}

data_ptr_t UndoBuffer::CreateEntry(UndoFlags type, idx_t len) {
	D_ASSERT(len <= NumericLimits<uint32_t>::Maximum());
	idx_t needed_space = AlignLength(len + UNDO_ENTRY_HEADER_SIZE);
	if (head->current_position + needed_space >= head->maximum_size) {
		auto new_chunk =
		    make_unique<UndoChunk>(needed_space > DEFAULT_UNDO_CHUNK_SIZE ? needed_space : DEFAULT_UNDO_CHUNK_SIZE);
		head->prev = new_chunk.get();
		new_chunk->next = move(head);
		head = move(new_chunk);
	}
	return head->WriteEntry(type, len);
}

template <class T>
void UndoBuffer::IterateEntries(UndoBuffer::IteratorState &state, T &&callback) {
	// iterate in insertion order: start with the tail
	state.current = tail;
	while (state.current) {
		state.start = state.current->data.get();
		state.end = state.start + state.current->current_position;
		while (state.start < state.end) {
			UndoFlags type = Load<UndoFlags>(state.start);
			state.start += sizeof(UndoFlags);

			uint32_t len = Load<uint32_t>(state.start);
			state.start += sizeof(uint32_t);
			callback(type, state.start);
			state.start += len;
		}
		state.current = state.current->prev;
	}
}

template <class T>
void UndoBuffer::IterateEntries(UndoBuffer::IteratorState &state, UndoBuffer::IteratorState &end_state, T &&callback) {
	// iterate in insertion order: start with the tail
	state.current = tail;
	while (state.current) {
		state.start = state.current->data.get();
		state.end =
		    state.current == end_state.current ? end_state.start : state.start + state.current->current_position;
		while (state.start < state.end) {
			auto type = Load<UndoFlags>(state.start);
			state.start += sizeof(UndoFlags);
			auto len = Load<uint32_t>(state.start);
			state.start += sizeof(uint32_t);
			callback(type, state.start);
			state.start += len;
		}
		if (state.current == end_state.current) {
			// finished executing until the current end state
			return;
		}
		state.current = state.current->prev;
	}
}

template <class T>
void UndoBuffer::ReverseIterateEntries(T &&callback) {
	// iterate in reverse insertion order: start with the head
	auto current = head.get();
	while (current) {
		data_ptr_t start = current->data.get();
		data_ptr_t end = start + current->current_position;
		// create a vector with all nodes in this chunk
		vector<pair<UndoFlags, data_ptr_t>> nodes;
		while (start < end) {
			auto type = Load<UndoFlags>(start);
			start += sizeof(UndoFlags);
			auto len = Load<uint32_t>(start);
			start += sizeof(uint32_t);
			nodes.emplace_back(type, start);
			start += len;
		}
		// iterate over it in reverse order
		for (idx_t i = nodes.size(); i > 0; i--) {
			callback(nodes[i - 1].first, nodes[i - 1].second);
		}
		current = current->next.get();
	}
}

bool UndoBuffer::ChangesMade() {
	return head->maximum_size > 0;
}

idx_t UndoBuffer::EstimatedSize() {
	idx_t estimated_size = 0;
	auto node = head.get();
	while (node) {
		estimated_size += node->current_position;
		node = node->next.get();
	}
	return estimated_size;
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
	UndoBuffer::IteratorState iterator_state;
	IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CleanupEntry(type, data); });
}

void UndoBuffer::Commit(UndoBuffer::IteratorState &iterator_state, WriteAheadLog *log, transaction_t commit_id) {
	CommitState state(commit_id, log);
	if (log) {
		// commit WITH write ahead log
		IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CommitEntry<true>(type, data); });
	} else {
		// commit WITHOUT write ahead log
		IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CommitEntry<false>(type, data); });
	}
}

void UndoBuffer::RevertCommit(UndoBuffer::IteratorState &end_state, transaction_t transaction_id) {
	CommitState state(transaction_id, nullptr);
	UndoBuffer::IteratorState start_state;
	IterateEntries(start_state, end_state, [&](UndoFlags type, data_ptr_t data) { state.RevertCommit(type, data); });
}

void UndoBuffer::Rollback() noexcept {
	// rollback needs to be performed in reverse
	RollbackState state;
	ReverseIterateEntries([&](UndoFlags type, data_ptr_t data) { state.RollbackEntry(type, data); });
}
} // namespace duckdb
