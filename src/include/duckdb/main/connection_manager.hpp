//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/connection_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;
class DatabaseInstance;

class ConnectionManager {
public:
	ConnectionManager();

	void AddConnection(ClientContext &context);
	void RemoveConnection(ClientContext &context);

	vector<shared_ptr<ClientContext>> GetConnectionList();
	const reference_map_t<ClientContext, weak_ptr<ClientContext>> &GetConnectionListReference() const {
		return connections;
	}
	idx_t GetConnectionCount() const;

	static ConnectionManager &Get(DatabaseInstance &db);
	static ConnectionManager &Get(ClientContext &context);

private:
	mutable mutex connections_lock;
	reference_map_t<ClientContext, weak_ptr<ClientContext>> connections;
};

} // namespace duckdb
