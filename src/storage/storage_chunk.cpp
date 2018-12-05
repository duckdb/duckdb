#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "storage/data_table.hpp"
#include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

StorageChunk::StorageChunk(DataTable &_table, size_t start) : table(_table), count(0), start(start), read_count(0) {
	columns.resize(table.types.size());
	size_t tuple_size = 0;
	for (auto &type : table.types) {
		tuple_size += GetTypeIdSize(type);
	}
	owned_data = unique_ptr<char[]>(new char[tuple_size * STORAGE_CHUNK_SIZE]);
	char *dataptr = owned_data.get();
	for (size_t i = 0; i < table.types.size(); i++) {
		columns[i] = dataptr;
		dataptr += GetTypeIdSize(table.types[i]) * STORAGE_CHUNK_SIZE;
	}
}

void StorageChunk::Cleanup(VersionInformation *info) {
	size_t entry = info->prev.entry;
	version_pointers[entry] = info->next;
	if (version_pointers[entry]) {
		version_pointers[entry]->prev.entry = entry;
		version_pointers[entry]->chunk = this;
	}
}

void StorageChunk::Undo(VersionInformation *info) {
	size_t entry = info->prev.entry;
	assert(version_pointers[entry] == info);
	if (!info->tuple_data) {
		deleted[entry] = true;
	} else {
		// move data back to the original chunk
		deleted[entry] = false;
		auto tuple_data = info->tuple_data;
		table.serializer.Deserialize(columns, entry, tuple_data);
	}
	version_pointers[entry] = info->next;
	if (version_pointers[entry]) {
		version_pointers[entry]->prev.entry = entry;
		version_pointers[entry]->chunk = this;
	}
}
