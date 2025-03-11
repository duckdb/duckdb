#include "duckdb/transaction/undo_buffer.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/transaction/cleanup_state.hpp"
#include "duckdb/transaction/commit_state.hpp"
#include "duckdb/transaction/rollback_state.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/transaction/wal_write_state.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {
constexpr uint32_t UNDO_ENTRY_HEADER_SIZE = sizeof(UndoFlags) + sizeof(uint32_t);

UndoBuffer::UndoBuffer(DuckTransaction &transaction_p, ClientContext &context_p)
    : transaction(transaction_p), allocator(BufferManager::GetBufferManager(context_p)) {
}

UndoBufferReference UndoBuffer::CreateEntry(UndoFlags type, idx_t len) {
	idx_t alloc_len = AlignValue<idx_t>(len + UNDO_ENTRY_HEADER_SIZE);
	auto handle = allocator.Allocate(alloc_len);
	auto data = handle.Ptr();
	// write the undo entry metadata
	Store<UndoFlags>(type, data);
	data += sizeof(UndoFlags);
	Store<uint32_t>(UnsafeNumericCast<uint32_t>(alloc_len - UNDO_ENTRY_HEADER_SIZE), data);
	// increment the position of the header past the undo entry metadata
	handle.position += UNDO_ENTRY_HEADER_SIZE;
	return handle;
}

template <class T>
void UndoBuffer::IterateEntries(UndoBuffer::IteratorState &state, T &&callback) {
	// iterate in insertion order: start with the tail
	state.current = allocator.tail.get();
	while (state.current) {
		state.handle = allocator.buffer_manager.Pin(state.current->block);
		state.start = state.handle.Ptr();
		state.end = state.start + state.current->position;
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
	state.current = allocator.tail.get();
	while (state.current) {
		state.handle = allocator.buffer_manager.Pin(state.current->block);
		state.start = state.handle.Ptr();
		state.end = state.current == end_state.current ? end_state.start : state.start + state.current->position;
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
	auto current = allocator.head.get();
	while (current) {
		auto handle = allocator.buffer_manager.Pin(current->block);
		data_ptr_t start = handle.Ptr();
		data_ptr_t end = start + current->position;
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
	// we need to search for any index creation entries
	return allocator.head.get();
}

UndoBufferProperties UndoBuffer::GetProperties() {
	UndoBufferProperties properties;
	if (!ChangesMade()) {
		return properties;
	}
	auto node = allocator.head.get();
	while (node) {
		properties.estimated_size += node->position;
		node = node->next.get();
	}

	// we need to search for any index creation entries
	IteratorState iterator_state;
	IterateEntries(iterator_state, [&](UndoFlags entry_type, data_ptr_t data) {
		switch (entry_type) {
		case UndoFlags::UPDATE_TUPLE:
			properties.has_updates = true;
			break;
		case UndoFlags::DELETE_TUPLE: {
			auto info = reinterpret_cast<DeleteInfo *>(data);
			if (info->is_consecutive) {
				properties.estimated_size += sizeof(row_t) * info->count;
			}
			if (info->table->HasIndexes()) {
				properties.has_index_deletes = true;
			}
			properties.has_deletes = true;
			break;
		}
		case UndoFlags::CATALOG_ENTRY: {
			properties.has_catalog_changes = true;

			auto catalog_entry = Load<CatalogEntry *>(data);
			auto &parent = catalog_entry->Parent();
			switch (parent.type) {
			case CatalogType::DELETED_ENTRY:
				properties.has_dropped_entries = true;
				break;
			case CatalogType::INDEX_ENTRY: {
				auto &index = parent.Cast<DuckIndexEntry>();
				properties.estimated_size += index.initial_index_size;
				break;
			}
			default:
				break;
			}
			break;
		}
		default:
			break;
		}
	});
	return properties;
}

void UndoBuffer::Cleanup(transaction_t lowest_active_transaction) {
	// garbage collect everything in the Undo Chunk
	// this should only happen if
	//  (1) the transaction this UndoBuffer belongs to has successfully
	//  committed
	//      (on Rollback the Rollback() function should be called, that clears
	//      the chunks)
	//  (2) there is no active transaction with start_id < commit_id of this
	//  transaction
	CleanupState state(lowest_active_transaction);
	UndoBuffer::IteratorState iterator_state;
	IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CleanupEntry(type, data); });

	// possibly vacuum indexes
	for (auto &table : state.indexed_tables) {
		table.second->VacuumIndexes();
	}
}

void UndoBuffer::WriteToWAL(WriteAheadLog &wal, optional_ptr<StorageCommitState> commit_state) {
	WALWriteState state(transaction, wal, commit_state);
	UndoBuffer::IteratorState iterator_state;
	IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CommitEntry(type, data); });
}

void UndoBuffer::Commit(UndoBuffer::IteratorState &iterator_state, transaction_t commit_id) {
	CommitState state(transaction, commit_id);
	IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CommitEntry(type, data); });
}

void UndoBuffer::RevertCommit(UndoBuffer::IteratorState &end_state, transaction_t transaction_id) {
	CommitState state(transaction, transaction_id);
	UndoBuffer::IteratorState start_state;
	IterateEntries(start_state, end_state, [&](UndoFlags type, data_ptr_t data) { state.RevertCommit(type, data); });
}

void UndoBuffer::Rollback() {
	// rollback needs to be performed in reverse
	RollbackState state(transaction);
	ReverseIterateEntries([&](UndoFlags type, data_ptr_t data) { state.RollbackEntry(type, data); });
}
} // namespace duckdb
