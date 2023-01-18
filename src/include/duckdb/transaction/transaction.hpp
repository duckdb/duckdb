//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/transaction/transaction_data.hpp"

namespace duckdb {
class SequenceCatalogEntry;
class SchemaCatalogEntry;

class AttachedDatabase;
class ColumnData;
class ClientContext;
class CatalogEntry;
class DataTable;
class DatabaseInstance;
class LocalStorage;
class MetaTransaction;
class TransactionManager;
class WriteAheadLog;

class ChunkVectorInfo;

struct DeleteInfo;
struct UpdateInfo;

//! The transaction object holds information about a currently running or past
//! transaction
class Transaction {
public:
	Transaction(TransactionManager &manager, ClientContext &context);
	virtual ~Transaction();

	TransactionManager &manager;
	weak_ptr<ClientContext> context;
	//! The current active query for the transaction. Set to MAXIMUM_QUERY_ID if
	//! no query is active.
	atomic<transaction_t> active_query;

public:
	static Transaction &Get(ClientContext &context, AttachedDatabase &db);
	static Transaction &Get(ClientContext &context, Catalog &catalog);

	virtual bool IsDTransaction() const {
		return false;
	}
};

} // namespace duckdb
