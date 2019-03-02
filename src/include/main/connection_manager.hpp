//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/connection_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "main/connection.hpp"

namespace duckdb {

class DuckDB;

class ConnectionManager {
public:
	void AddConnection(DuckDBConnection *conn) {
		assert(conn);
		std::lock_guard<std::mutex> lock(connections_lock);
		connections.insert(conn);
	}
	void RemoveConnection(DuckDBConnection *conn) {
		assert(conn);
		std::lock_guard<std::mutex> lock(connections_lock);
		connections.erase(conn);
	}

	template <class T> void Scan(T &&callback) {
		// lock the catalog set
		std::lock_guard<std::mutex> lock(connections_lock);
		for (auto &conn : connections) {
			callback(conn);
		}
	}

private:
	std::mutex connections_lock;
	unordered_set<DuckDBConnection *> connections;
};

} // namespace duckdb
