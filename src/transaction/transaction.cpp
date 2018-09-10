
#include "common/exception.hpp"

#include "catalog/column_definition.hpp"
#include "catalog/table_catalog.hpp"

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

void Transaction::PushDeletedEntries(
    size_t offset, size_t count, StorageChunk *storage,
    std::shared_ptr<VersionInformation> version_pointers[]) {
	for (size_t i = 0; i < count; i++) {
		auto ptr = PushTuple(0);
		auto meta = make_shared<VersionInformation>();
		meta->tuple_data = nullptr;
		meta->version_number = transaction_id;
		meta->prev.entry = offset + i;
		meta->chunk = storage;
		meta->next = nullptr;
		version_pointers[i] = meta;
		*((VersionInformation **)ptr) = meta.get();
	}
}

void Transaction::PushTuple(size_t offset, StorageChunk *storage) {
	// push the tuple into the undo buffer
	auto ptr = PushTuple(storage->table.tuple_size);
	auto tuple_data = ptr + sizeof(VersionInformation *);

	// create the meta data for the tuple
	auto meta = make_shared<VersionInformation>();
	meta->tuple_data = tuple_data;
	meta->version_number = transaction_id;
	meta->prev.entry = offset;
	meta->chunk = storage;
	meta->next = storage->version_pointers[offset];
	storage->version_pointers[offset] = meta;

	// fill in the tuple data
	// first fill in the version information
	*((VersionInformation **)ptr) = meta.get();
	// now fill in the tuple data
	for (size_t i = 0; i < storage->columns.size(); i++) {
		size_t value_size = GetTypeIdSize(storage->table.table.columns[i].type);
		void *storage_pointer = storage->columns[i].data + value_size * offset;

		memcpy(tuple_data, storage_pointer, value_size);

		tuple_data += value_size;
	}
}

uint8_t *Transaction::PushTuple(size_t data_size) {
	return undo_buffer.CreateEntry(UndoFlags::TUPLE_ENTRY,
	                               sizeof(VersionInformation *) + data_size);
}

void Transaction::Commit(transaction_t commit_id) {
	this->commit_id = commit_id;
	undo_buffer.Commit(commit_id);
}
