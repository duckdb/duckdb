//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/result_set_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/main/query_parameters.hpp"

namespace duckdb {

class ResultSetManager;
class PinnedResultSet;
class ClientContext;
class DatabaseInstance;
class ColumnDataCollection;

class ManagedResultSet : public enable_shared_from_this<ManagedResultSet> {
	friend class ResultSetManager;

private:
	ManagedResultSet(weak_ptr<DatabaseInstance> db, QueryResultMemoryType memory_type,
	                 unique_ptr<ColumnDataCollection> collection);

public:
	~ManagedResultSet();

public:
	unique_ptr<PinnedResultSet> Pin();

private:
	weak_ptr<DatabaseInstance> db;
	const QueryResultMemoryType memory_type;
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataScanState scan_state;
};

class PinnedResultSet {
	friend class ManagedResultSet;

private:
	PinnedResultSet(shared_ptr<DatabaseInstance> db_p, shared_ptr<ManagedResultSet> result_set_p,
	                ColumnDataCollection &collection_p, ColumnDataScanState &scan_state_p)
	    : db(std::move(db_p)), result_set(std::move(result_set_p)), collection(collection_p), scan_state(scan_state_p) {
	}

public:
	unique_ptr<PinnedResultSet> Copy() const {
		return unique_ptr<PinnedResultSet>(new PinnedResultSet(db, result_set, collection, scan_state));
	}

private:
	shared_ptr<DatabaseInstance> db;
	shared_ptr<ManagedResultSet> result_set;

public:
	ColumnDataCollection &collection;
	ColumnDataScanState &scan_state;
};

class ResultSetManager {
	friend class ManagedResultSet;

public:
	explicit ResultSetManager(DatabaseInstance &db);
	~ResultSetManager();

public:
	static ResultSetManager &Get(ClientContext &context);
	shared_ptr<ManagedResultSet> Add(unique_ptr<ColumnDataCollection> collection, QueryResultMemoryType memory_type);

private:
	void Remove(ManagedResultSet &query_result);

private:
	mutex lock;
	weak_ptr<DatabaseInstance> db;
	reference_map_t<ManagedResultSet, weak_ptr<ManagedResultSet>> open_results;
};

} // namespace duckdb
