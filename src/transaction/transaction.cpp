
#include "common/exception.hpp"

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
