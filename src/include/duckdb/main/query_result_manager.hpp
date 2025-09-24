//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_result_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class QueryResultManager;
class ClientContext;
class DatabaseInstance;
class ColumnDataCollection;

class ManagedQueryResult {
	friend class QueryResultManager;

private:
	ManagedQueryResult(weak_ptr<DatabaseInstance> db, unique_ptr<ColumnDataCollection> collection);

public:
	~ManagedQueryResult();

public:
	ColumnDataCollection &Collection();
	ColumnDataCollection &Collection() const;
	ColumnDataScanState &ScanState();

private:
	void ValidateResult() const;

private:
	weak_ptr<DatabaseInstance> db;
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataScanState scan_state;
};

class QueryResultManager {
	friend class ManagedQueryResult;

public:
	explicit QueryResultManager(DatabaseInstance &db);
	~QueryResultManager();

public:
	static QueryResultManager &Get(ClientContext &context);
	shared_ptr<ManagedQueryResult> Add(unique_ptr<ColumnDataCollection> collection);

private:
	void Remove(ManagedQueryResult &query_result);

private:
	mutex lock;
	weak_ptr<DatabaseInstance> db;
	reference_map_t<ManagedQueryResult, weak_ptr<ManagedQueryResult>> open_results;
};

} // namespace duckdb
