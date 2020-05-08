#include "duckdb/transaction/cleanup_state.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/update_info.hpp"

#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/uncompressed_segment.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

using namespace duckdb;
using namespace std;

CleanupState::CleanupState() : current_table(nullptr), count(0) {
}

CleanupState::~CleanupState() {
	Flush();
}

void CleanupState::CleanupEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		CatalogEntry *catalog_entry = *((CatalogEntry **)data);
		// destroy the backed up entry: it is no longer required
		assert(catalog_entry->parent);
		if (catalog_entry->parent->type != CatalogType::UPDATED_ENTRY) {
			if (!catalog_entry->parent->child->deleted) {
				// delete the entry from the dependency manager, if it is not deleted yet
				catalog_entry->catalog->dependency_manager.EraseObject(catalog_entry->parent->child.get());
			}
			catalog_entry->parent->child = move(catalog_entry->child);
		}
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		auto info = (DeleteInfo *)data;
		CleanupDelete(info);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		auto info = (UpdateInfo *)data;
		CleanupUpdate(info);
		break;
	}
	default:
		break;
	}
}

void CleanupState::CleanupUpdate(UpdateInfo *info) {
	// remove the update info from the update chain
	// first obtain an exclusive lock on the segment
	auto lock = info->segment->lock.GetExclusiveLock();
	info->segment->CleanupUpdate(info);
}

void CleanupState::CleanupDelete(DeleteInfo *info) {
	auto version_table = info->table;
	if (version_table->info->indexes.size() == 0) {
		// this table has no indexes: no cleanup to be done
		return;
	}
	if (current_table != version_table) {
		// table for this entry differs from previous table: flush and switch to the new table
		Flush();
		current_table = version_table;
	}
	for (idx_t i = 0; i < info->count; i++) {
		if (count == STANDARD_VECTOR_SIZE) {
			Flush();
		}
		row_numbers[count++] = info->vinfo->start + info->rows[i];
	}
}

void CleanupState::Flush() {
	if (count == 0) {
		return;
	}

	// set up the row identifiers vector
	Vector row_identifiers(ROW_TYPE, (data_ptr_t)row_numbers);

	// delete the tuples from all the indexes
	current_table->RemoveFromIndexes(row_identifiers, count);

	count = 0;
}
