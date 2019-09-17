#include "transaction/transaction.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "parser/column_definition.hpp"
#include "storage/data_table.hpp"
#include "storage/write_ahead_log.hpp"

#include "transaction/version_info.hpp"
#include <cstring>

using namespace duckdb;
using namespace std;

void Transaction::PushCatalogEntry(CatalogEntry *entry) {
	// store only the pointer to the catalog entry
	CatalogEntry **blob = (CatalogEntry **)undo_buffer.CreateEntry(UndoFlags::CATALOG_ENTRY, sizeof(CatalogEntry *));
	*blob = entry;
}

void Transaction::PushDelete(ChunkInfo *vinfo, row_t rows[], index_t count) {
	auto delete_info = (DeleteInfo*) undo_buffer.CreateEntry(UndoFlags::DELETE_TUPLE, sizeof(DeleteInfo) + sizeof(row_t) * count);
	delete_info->vinfo = vinfo;
	delete_info->count = count;
	memcpy(delete_info->rows, rows, count * STANDARD_VECTOR_SIZE);
}

void Transaction::PushQuery(string query) {
	char *blob = (char *)undo_buffer.CreateEntry(UndoFlags::QUERY, query.size() + 1);
	strcpy(blob, query.c_str());
}

void Transaction::CheckCommit() {
	storage.CheckCommit();
}

void Transaction::Commit(WriteAheadLog *log, transaction_t commit_id) noexcept {
	this->commit_id = commit_id;

	// commit the undo buffer
	bool changes_made = undo_buffer.ChangesMade() || storage.ChangesMade() || sequence_usage.size() > 0;
	undo_buffer.Commit(log, commit_id);
	storage.Commit(*this, log, commit_id);
	if (log) {
		// commit any sequences that were used to the WAL
		for (auto &entry : sequence_usage) {
			log->WriteSequenceValue(entry.first, entry.second);
		}
		// flush the WAL
		if (changes_made) {
			log->Flush();
		}
	}
}
