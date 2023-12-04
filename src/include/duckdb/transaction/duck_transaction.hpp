//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/duck_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class RowVersionManager;

class DuckTransaction : public Transaction {
public:
	DuckTransaction(TransactionManager &manager, ClientContext &context, transaction_t start_time,
	                transaction_t transaction_id);
	~DuckTransaction() override;

	//! The start timestamp of this transaction
	transaction_t start_time;
	//! The transaction id of this transaction
	transaction_t transaction_id;
	//! The commit id of this transaction, if it has successfully been committed
	transaction_t commit_id;
	//! Map of all sequences that were used during the transaction and the value they had in this transaction
	unordered_map<SequenceCatalogEntry *, SequenceValue> sequence_usage;
	//! Highest active query when the transaction finished, used for cleaning up
	transaction_t highest_active_query;

public:
	static DuckTransaction &Get(ClientContext &context, AttachedDatabase &db);
	static DuckTransaction &Get(ClientContext &context, Catalog &catalog);
	LocalStorage &GetLocalStorage();

	void PushCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data = nullptr, idx_t extra_data_size = 0);

	//! Commit the current transaction with the given commit identifier. Returns an error message if the transaction
	//! commit failed, or an empty string if the commit was sucessful
	string Commit(AttachedDatabase &db, transaction_t commit_id, bool checkpoint) noexcept;
	//! Returns whether or not a commit of this transaction should trigger an automatic checkpoint
	bool AutomaticCheckpoint(AttachedDatabase &db);

	//! Rollback
	void Rollback() noexcept;
	//! Cleanup the undo buffer
	void Cleanup();

	bool ChangesMade();

	void PushDelete(DataTable &table, RowVersionManager &info, idx_t vector_idx, row_t rows[], idx_t count,
	                idx_t base_row);
	void PushAppend(DataTable &table, idx_t row_start, idx_t row_count);
	UpdateInfo *CreateUpdateInfo(idx_t type_size, idx_t entries);

	bool IsDuckTransaction() const override {
		return true;
	}

private:
	//! The undo buffer is used to store old versions of rows that are updated
	//! or deleted
	UndoBuffer undo_buffer;
	//! The set of uncommitted appends for the transaction
	unique_ptr<LocalStorage> storage;
};

} // namespace duckdb
