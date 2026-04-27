//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_snapshot_registry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class MetaTransaction;

//! Process-wide-per-DatabaseInstance registry of shared transactions.
//! The owning ClientContext exports its in-progress MetaTransaction with Export(),
//! receiving an opaque snapshot id. Other ClientContexts on the same DatabaseInstance
//! can then call Import() with that id to obtain a shared_ptr<MetaTransaction> they
//! can adopt as participants.
class TransactionSnapshotRegistry {
public:
	TransactionSnapshotRegistry();
	~TransactionSnapshotRegistry();

	//! Export a transaction. Returns a freshly generated id. The transaction is marked
	//! shared. The registry holds a weak_ptr so it does not keep the transaction alive
	//! beyond the owner connection's transaction lifetime.
	string Export(shared_ptr<MetaTransaction> &transaction);

	//! Import a previously exported transaction. Returns the shared_ptr if the id is
	//! still valid (transaction not yet finalized and not force-detached). Throws on
	//! unknown / expired / detached ids.
	shared_ptr<MetaTransaction> Import(const string &snapshot_id);

	//! Remove a snapshot id from the registry (called when the owner finalizes its txn).
	void Unregister(const string &snapshot_id);

private:
	mutex registry_lock;
	unordered_map<string, weak_ptr<MetaTransaction>> snapshots;
};

} // namespace duckdb
