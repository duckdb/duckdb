#include "transaction/version_info.hpp"
#include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

DataTable &DeleteInfo::GetTable() {
	return vinfo->manager.table;
}

// index_t DeleteInfo::GetRowId() {
// 	return vinfo->chunk.start + vinfo->start + row_id;
// }

bool Versioning::HasConflict(transaction_t version_number, transaction_t transaction_id) {
	if (version_number < TRANSACTION_ID_START) {
		// version was committed: no conflict
		return false;
	}
	if (version_number == transaction_id) {
		// version belongs to this transaction: no conflict
		return false;
	}
	// version was not committed but belongs to other transaction: conflict
	return true;
}

bool Versioning::UseVersion(Transaction &transaction, transaction_t id) {
	return id < transaction.start_time || id == transaction.transaction_id;
}
