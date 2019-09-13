#include "transaction/cleanup_state.hpp"
#include "transaction/version_info.hpp"

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
		throw Exception("FIXME: cleanup update");
	}
	case UndoFlags::QUERY:
		break;
	default:
		break;
	}
}

void CleanupState::CleanupDelete(DeleteInfo *info) {
	auto version_table = &info->GetTable();
	if (version_table->indexes.size() == 0) {
		// this table has no indexes: no cleanup to be done
		return;
	}
	if (current_table != version_table || flag != UndoFlags::DELETE_TUPLE) {
		// table for this entry differs from previous table: flush and switch to the new table
		Flush();
		flag = UndoFlags::DELETE_TUPLE;
		current_table = version_table;
	}
	if (count == STANDARD_VECTOR_SIZE) {
		// current vector is filled up: flush
		Flush();
	}

	row_numbers[count] = info->GetRowId();
	count++;
}

void CleanupState::Flush() {
	if (count == 0) {
		return;
	}

	// set up the row identifiers vector
	Vector row_identifiers(ROW_TYPE, (data_ptr_t)row_numbers);
	row_identifiers.count = count;

	if (flag == UndoFlags::UPDATE_TUPLE) {
		// // retrieve data from the version info
		// current_table->RetrieveVersionedData(chunk, data, count);
		// // delete it from all the indexes
		// current_table->RemoveFromIndexes(chunk, row_identifiers);
		// chunk.Reset();
	} else {
		// delete the tuples from all the indexes
		current_table->RemoveFromIndexes(row_identifiers);
	}

	count = 0;
}
