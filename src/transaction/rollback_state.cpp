#include "transaction/rollback_state.hpp"

using namespace duckdb;
using namespace std;

static void RollbackIndexInsert(VersionInfo *info);

void RollbackState::RollbackEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// undo this catalog entry
		CatalogEntry *catalog_entry = *((CatalogEntry **)data);
		assert(catalog_entry->set);
		catalog_entry->set->Undo(catalog_entry);
		break;
	}
	case UndoFlags::DELETE_TUPLE:
	case UndoFlags::UPDATE_TUPLE:
	case UndoFlags::INSERT_TUPLE: {
		// undo this entry
		auto info = (VersionInfo *)data;
		if (type == UndoFlags::UPDATE_TUPLE || type == UndoFlags::INSERT_TUPLE) {
			// update or insert rolled back
			// delete base table entry from index
			assert(!info->prev);
			if (info->GetTable().indexes.size() > 0) {
				RollbackIndexInsert(info);
			}
		}
		// parent needs to refer to a storage chunk because of our transactional model
		// the current entry is still dirty, hence no other transaction can have modified it
		info->vinfo->Undo(info);
		break;
	}
	case UndoFlags::QUERY:
		break;
	default:
		assert(type == UndoFlags::EMPTY_ENTRY);
		break;
	}
}

static void RollbackIndexInsert(VersionInfo *info) {
	row_t row_id = info->GetRowId();
	Value ptr = Value::BIGINT(row_id);
	Vector row_identifiers(ptr);

	DataChunk result;
	result.Initialize(info->GetTable().types);
	info->vinfo->chunk.AppendToChunk(result, info);
	for (auto &index : info->GetTable().indexes) {
		index->Delete(result, row_identifiers);
	}
}
