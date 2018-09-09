
#include "common/exception.hpp"

#include "storage/data_table.hpp"

#include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

void Transaction::PushCatalogEntry(AbstractCatalogEntry *entry) {
	// store only the pointer to the catalog entry
	AbstractCatalogEntry **blob =
	    (AbstractCatalogEntry **)undo_buffer.CreateEntry(
	        UndoFlags::CATALOG_ENTRY, sizeof(AbstractCatalogEntry *));
	*blob = entry;
}

void Transaction::PushDeletedEntries(size_t offset, size_t count, StorageChunk *storage, std::shared_ptr<VersionInformation> version_pointers[]) {
	for(size_t i = 0; i < count; i++) {
		auto ptr = PushTuple(0);
		auto meta = make_shared<VersionInformation>();
		meta->tuple_data = nullptr;
		meta->version_number = transaction_id;
		meta->prev.entry = offset + i;
		meta->chunk = storage;
		meta->next = nullptr;
		version_pointers[i] = meta;
		*((VersionInformation**) ptr) = meta.get();
	}
}

void* Transaction::PushTuple(size_t data_size) {
	return (void*) undo_buffer.CreateEntry(UndoFlags::TUPLE_ENTRY, sizeof(VersionInformation*) + data_size); 
}

void Transaction::Commit(transaction_t commit_id) {
	this->commit_id = commit_id;
	undo_buffer.Commit(commit_id);
}
