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
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class DatabaseInstance;
class ClientContext;
class BlockHandle;
class ColumnDataAllocator;

class ManagedResultSet : public enable_shared_from_this<ManagedResultSet> {
public:
	ManagedResultSet();
	ManagedResultSet(const weak_ptr<DatabaseInstance> &db, vector<shared_ptr<BlockHandle>> &handles);

public:
	bool IsValid() const;
	shared_ptr<DatabaseInstance> GetDatabase() const;
	vector<shared_ptr<BlockHandle>> &GetHandles();

private:
	bool valid;
	weak_ptr<DatabaseInstance> db;
	optional_ptr<vector<shared_ptr<BlockHandle>>> handles;
};

class ResultSetManager {
public:
	explicit ResultSetManager(DatabaseInstance &db);

public:
	static ResultSetManager &Get(ClientContext &context);
	static ResultSetManager &Get(DatabaseInstance &db);
	ManagedResultSet Add(ColumnDataAllocator &allocator);
	void Remove(ColumnDataAllocator &allocator);

private:
	mutex lock;
	weak_ptr<DatabaseInstance> db;
	reference_map_t<ColumnDataAllocator, unique_ptr<vector<shared_ptr<BlockHandle>>>> open_results;
};

} // namespace duckdb
