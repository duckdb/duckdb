//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/result_set_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/main/query_parameters.hpp"

namespace duckdb {

class DatabaseInstance;
class ClientContext;
class BlockHandle;
class ColumnDataAllocator;

class ManagedResultSet : public enable_shared_from_this<ManagedResultSet> {
public:
	explicit ManagedResultSet(const weak_ptr<DatabaseInstance> &db);

public:
	weak_ptr<DatabaseInstance> db;
	vector<shared_ptr<BlockHandle>> handles;
};

class ResultSetManager {
public:
	explicit ResultSetManager(DatabaseInstance &db);

public:
	static ResultSetManager &Get(ClientContext &context);
	static ResultSetManager &Get(DatabaseInstance &db);
	optional_ptr<ManagedResultSet> Add(ColumnDataAllocator &allocator);
	void Remove(ColumnDataAllocator &allocator);

private:
	mutex lock;
	weak_ptr<DatabaseInstance> db;
	reference_map_t<ColumnDataAllocator, unique_ptr<ManagedResultSet>> open_results;
};

} // namespace duckdb
