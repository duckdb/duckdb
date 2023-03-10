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
	ConnectionManager() {
	}

	void AddConnection(ClientContext &context) {
		lock_guard<mutex> lock(connections_lock);
		connections.insert(make_pair(&context, weak_ptr<ClientContext>(context.shared_from_this())));
	}

	void RemoveConnection(ClientContext &context) {
		lock_guard<mutex> lock(connections_lock);
		connections.erase(&context);
	}

	vector<shared_ptr<ClientContext>> GetConnectionList() {
		vector<shared_ptr<ClientContext>> result;
		for (auto &it : connections) {
			auto connection = it.second.lock();
			if (!connection) {
				connections.erase(it.first);
				continue;
			} else {
				result.push_back(std::move(connection));
			}
		}

		return result;
	}

	ClientContext *GetConnection(DatabaseInstance *db);

	static ConnectionManager &Get(DatabaseInstance &db);
	static ConnectionManager &Get(ClientContext &context);

public:
	mutex connections_lock;
	unordered_map<ClientContext *, weak_ptr<ClientContext>> connections;
};

} // namespace duckdb
