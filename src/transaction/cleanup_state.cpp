#include "duckdb/transaction/cleanup_state.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/update_info.hpp"

#include "duckdb/storage/data_table.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"

namespace duckdb {

CleanupState::CleanupState() : current_table(nullptr), count(0) {
}

CleanupState::~CleanupState() {
	Flush();
}

void CleanupState::CleanupEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry);
		D_ASSERT(catalog_entry->set);
		catalog_entry->set->CleanupEntry(*catalog_entry);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		auto info = reinterpret_cast<DeleteInfo *>(data);
		CleanupDelete(*info);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		auto info = reinterpret_cast<UpdateInfo *>(data);
		CleanupUpdate(*info);
		break;
	}
	default:
		break;
	}
}

void CleanupState::CleanupUpdate(UpdateInfo &info) {
	// remove the update info from the update chain
	// first obtain an exclusive lock on the segment
	info.segment->CleanupUpdate(info);
}

void CleanupState::CleanupDelete(DeleteInfo &info) {
	auto version_table = info.table;
	D_ASSERT(version_table->info->cardinality >= info.count);
	version_table->info->cardinality -= info.count;

	if (version_table->info->indexes.Empty()) {
		// this table has no indexes: no cleanup to be done
		return;
	}

	if (current_table != version_table) {
		// table for this entry differs from previous table: flush and switch to the new table
		Flush();
		current_table = version_table;
	}

	// possibly vacuum any indexes in this table later
	indexed_tables[current_table->info->table] = current_table;

	count = 0;
	for (idx_t i = 0; i < info.count; i++) {
		row_numbers[count++] = info.base_row + info.rows[i];
	}
	Flush();
}

void CleanupState::Flush() {
	if (count == 0) {
		return;
	}

	// set up the row identifiers vector
	Vector row_identifiers(LogicalType::ROW_TYPE, data_ptr_cast(row_numbers));

	// delete the tuples from all the indexes
	try {
		current_table->RemoveFromIndexes(row_identifiers, count);
	} catch (...) {
	}

	count = 0;
}

} // namespace duckdb
