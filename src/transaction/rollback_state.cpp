#include "transaction/rollback_state.hpp"

using namespace duckdb;
using namespace std;

void RollbackState::RollbackEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// undo this catalog entry
		CatalogEntry *catalog_entry = *((CatalogEntry **)data);
		assert(catalog_entry->set);
		catalog_entry->set->Undo(catalog_entry);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		auto info = (DeleteInfo *)data;
		// reset the deleted flag on rollback
		info->vinfo->deleted[info->row_id] = NOT_DELETED_ID;
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		throw Exception("FIXME: rollback insert not supported");
	}
	case UndoFlags::QUERY:
		break;
	default:
		assert(type == UndoFlags::EMPTY_ENTRY);
		break;
	}
}
