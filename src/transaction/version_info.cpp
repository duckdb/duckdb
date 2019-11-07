#include "duckdb/transaction/version_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/storage/table/version_chunk_info.hpp"
#include "duckdb/storage/table/version_chunk.hpp"

using namespace duckdb;
using namespace std;

DataTable &VersionInfo::GetTable() {
	return vinfo->chunk.table;
}

index_t VersionInfo::GetRowId() {
	return vinfo->chunk.start + vinfo->start + entry;
}

VersionInfo *VersionInfo::GetVersionForTransaction(Transaction &transaction, VersionInfo *version) {
	if (!version ||
	    (version->version_number == transaction.transaction_id || version->version_number < transaction.start_time)) {
		// either (1) there is no version anymore as it was cleaned up,
		// or (2) the base table data belongs to the current transaction
		// in this case use the data in the original table
		return nullptr;
	} else {
		// follow the version pointers
		while (true) {
			auto next = version->next;
			if (!next) {
				// use this version: no predecessor
				break;
			}
			if (next->version_number == transaction.transaction_id) {
				// use this version: it was created by us
				break;
			}
			if (next->version_number < transaction.start_time) {
				// use this version: it was committed by us
				break;
			}
			version = next;
		}
		return version;
	}
}

bool VersionInfo::HasConflict(VersionInfo *version, transaction_t transaction_id) {
	if (!version) {
		return false;
	}
	if (version->version_number < TRANSACTION_ID_START) {
		// version was committed: no conflict
		return false;
	}
	if (version->version_number == transaction_id) {
		// version belongs to this transaction: no conflict
		return false;
	}
	// version was not committed but belongs to other transaction: conflict
	return true;
}
