#include "duckdb/main/connection_manager.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/extension_callback.hpp"

namespace duckdb {

ConnectionManager::ConnectionManager() {
}

void ConnectionManager::AddConnection(ClientContext &context) {
	lock_guard<mutex> lock(connections_lock);
	for (auto &callback : DBConfig::GetConfig(context).extension_callbacks) {
		callback->OnConnectionOpened(context);
	}
	connections[context] = weak_ptr<ClientContext>(context.shared_from_this());
}

void ConnectionManager::RemoveConnection(ClientContext &context) {
	lock_guard<mutex> lock(connections_lock);
	for (auto &callback : DBConfig::GetConfig(context).extension_callbacks) {
		callback->OnConnectionClosed(context);
	}
	connections.erase(context);
}

idx_t ConnectionManager::GetConnectionCount() const {
	lock_guard<mutex> lock(connections_lock);
	return connections.size();
}

vector<shared_ptr<ClientContext>> ConnectionManager::GetConnectionList() {
	lock_guard<mutex> lock(connections_lock);
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

} // namespace duckdb
