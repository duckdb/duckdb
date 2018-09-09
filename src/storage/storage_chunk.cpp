
#include "storage/data_table.hpp"

#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/types/vector_operations.hpp"

#include "catalog/table_catalog.hpp"

#include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

StorageChunk::StorageChunk(vector<ColumnDefinition>& table_columns) : 
	read_count(0), count(0) {
	columns.resize(table_columns.size());
	size_t tuple_size = 0;
	for(auto &column : table_columns) {
		tuple_size += GetTypeIdSize(column.type);
	}
	owned_data = unique_ptr<char[]>(new char[tuple_size * STORAGE_CHUNK_SIZE]);
	char *dataptr = owned_data.get();
	for(size_t i = 0; i < table_columns.size(); i++) {
		columns[i].data = dataptr;
		dataptr += GetTypeIdSize(table_columns[i].type) * STORAGE_CHUNK_SIZE;
	}
}

void StorageChunk::Cleanup(VersionInformation *info) {
	size_t entry = info->prev.entry;
	version_pointers[entry] = info->next;
}

void StorageChunk::Undo(VersionInformation *info) {
	size_t entry = info->prev.entry;
	assert(version_pointers[entry].get() == info);
	if (!info->tuple_data) {
		deleted[entry] = true;
	} else {
		throw NotImplementedException("Not implemented: move data back from deleted version pointer");
	}
	version_pointers[entry] = info->next;
}
