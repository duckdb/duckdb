#include "duckdb/transaction/transaction_snapshot_registry.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

TransactionSnapshotRegistry::TransactionSnapshotRegistry() {
}

TransactionSnapshotRegistry::~TransactionSnapshotRegistry() {
}

string TransactionSnapshotRegistry::Export(shared_ptr<MetaTransaction> &transaction) {
	if (!transaction) {
		throw InternalException("TransactionSnapshotRegistry::Export called with null transaction");
	}
	transaction->MarkShared();

	lock_guard<mutex> guard(registry_lock);
	// Generate a unique id; collision is astronomically unlikely but loop defensively.
	for (idx_t attempt = 0; attempt < 8; attempt++) {
		auto raw = UUID::GenerateRandomUUID();
		auto id = UUID::ToString(raw);
		if (snapshots.find(id) == snapshots.end()) {
			snapshots.emplace(id, weak_ptr<MetaTransaction>(transaction));
			return id;
		}
	}
	throw InternalException("Failed to generate a unique transaction snapshot id");
}

shared_ptr<MetaTransaction> TransactionSnapshotRegistry::Import(const string &snapshot_id) {
	lock_guard<mutex> guard(registry_lock);
	auto entry = snapshots.find(snapshot_id);
	if (entry == snapshots.end()) {
		throw TransactionException("transaction snapshot \"%s\" does not exist or has expired", snapshot_id);
	}
	auto shared = entry->second.lock();
	if (!shared) {
		// owner already finalized; clean up the stale entry
		snapshots.erase(entry);
		throw TransactionException("transaction snapshot \"%s\" has expired (owner committed or rolled back)",
		                           snapshot_id);
	}
	if (shared->IsForceDetached()) {
		throw TransactionException("transaction snapshot \"%s\" was rolled back by its owner", snapshot_id);
	}
	if (shared->IsCommitting()) {
		throw TransactionException(
		    "transaction snapshot \"%s\" cannot be imported: the owner is committing this transaction", snapshot_id);
	}
	return shared;
}

void TransactionSnapshotRegistry::Unregister(const string &snapshot_id) {
	lock_guard<mutex> guard(registry_lock);
	snapshots.erase(snapshot_id);
}

} // namespace duckdb
