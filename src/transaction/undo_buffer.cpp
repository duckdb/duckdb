
#include "transaction/undo_buffer.hpp"

#include "common/exception.hpp"

#include "catalog/abstract_catalog.hpp"
#include "catalog/catalog_set.hpp"

using namespace duckdb;
using namespace std;

const uint8_t *UndoBuffer::CreateEntry(UndoFlags type, size_t len) {
	UndoEntry entry;
	entry.type = type;
	entry.length = len;
	auto dataptr = new uint8_t[len];
	entry.data = unique_ptr<uint8_t[]>(dataptr);
	entries.push_back(move(entry));
	return dataptr;
}

UndoBuffer::~UndoBuffer() {
	// garbage collect everything in the Undo Chunk
	// this should only happen if
	//  (1) the transaction this UndoBuffer belongs to has successfully
	//  committed
	//      (on Rollback the Rollback() function should be called, that clears
	//      the chunks)
	//  (2) there is no active transaction with start_id < commit_id of this
	//  transaction
	for (auto &entry : entries) {
		if (entry.type == UndoFlags::CATALOG_ENTRY) {
			AbstractCatalogEntry *catalog_entry =
			    *((AbstractCatalogEntry **)entry.data.get());
			// destroy the backed up entry: it is no longer required
			assert(catalog_entry->parent);
			catalog_entry->parent->child = nullptr;
		} else {
			throw Exception("UndoBuffer - don't know how to garbage collect "
			                "this type yet!");
		}
	}
}

void UndoBuffer::Commit(transaction_t commit_id) {
	for (auto &entry : entries) {
		if (entry.type == UndoFlags::CATALOG_ENTRY) {
			// set the commit timestamp of the catalog entry to the given id
			AbstractCatalogEntry *catalog_entry =
			    *((AbstractCatalogEntry **)entry.data.get());
			assert(catalog_entry->parent);
			catalog_entry->parent->timestamp = commit_id;
		} else {
			throw Exception(
			    "UndoBuffer - don't know how to commit this type yet!");
		}
	}
}

void UndoBuffer::Rollback() {
	for(size_t i = entries.size(); i > 0; i--) {
		auto &entry = entries[i - 1];
		if (entry.type == UndoFlags::CATALOG_ENTRY) {
			// set the commit timestamp of the catalog entry to the given id
			AbstractCatalogEntry *catalog_entry =
			    *((AbstractCatalogEntry **)entry.data.get());
			assert(catalog_entry->set);
			catalog_entry->set->Undo(catalog_entry);
		} else {
			throw Exception(
			    "UndoBuffer - don't know how to rollback this type yet!");
		}
	}
	entries.clear();
}
