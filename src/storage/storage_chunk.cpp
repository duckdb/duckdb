#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "storage/data_table.hpp"
#include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

StorageChunk::StorageChunk(DataTable &base_table, index_t start) :
	SegmentBase(start, 0), table(base_table) {
}

void StorageChunk::Cleanup(VersionInformation *info) {
	index_t entry = info->prev.entry;
	version_pointers[entry] = info->next;
	if (version_pointers[entry]) {
		version_pointers[entry]->prev.entry = entry;
		version_pointers[entry]->chunk = this;
	}
}

void StorageChunk::Undo(VersionInformation *info) {
	index_t entry = info->prev.entry;
	assert(version_pointers[entry] == info);
	if (!info->tuple_data) {
		deleted[entry] = true;
	} else {
		// move data back to the original chunk
		deleted[entry] = false;
		auto tuple_data = info->tuple_data;

		// data_ptr_t dataptr = current_chunk->GetPointerToRow(column_ids[j], current_chunk->start + current_offset);
		throw NotImplementedException("Undo");
		//table.serializer.Deserialize(columns, entry, tuple_data);
	}
	version_pointers[entry] = info->next;
	if (version_pointers[entry]) {
		version_pointers[entry]->prev.entry = entry;
		version_pointers[entry]->chunk = this;
	}
}

data_ptr_t StorageChunk::GetPointerToRow(index_t col, index_t row) {
	return columns[col].segment->GetPointerToRow(table.types[col], row);
}
