#include "duckdb/main/connection_manager.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"

namespace duckdb {

ConnectionManager::ConnectionManager() : is_locking(false) {
}

void ConnectionManager::AddConnection(ClientContext &context) {
	lock_guard<mutex> lock(connections_lock);
	connections.insert(make_pair(&context, weak_ptr<ClientContext>(context.shared_from_this())));
}

void ConnectionManager::RemoveConnection(ClientContext &context) {
	lock_guard<mutex> lock(connections_lock);
	connections.erase(&context);
}

vector<shared_ptr<ClientContext>> ConnectionManager::GetConnectionList() {
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

void ConnectionManager::LockClients(vector<ClientLockWrapper> &client_locks, ClientContext &context) {
	{
		lock_guard<mutex> l(lock_clients_lock);
		if (is_locking) {
			throw TransactionException("Failed to lock clients - another thread is running FORCE CHECKPOINT");
		}
		is_locking = true;
	}
	client_locks.emplace_back(connections_lock, nullptr);
	auto connection_list = GetConnectionList();
	for (auto &con : connection_list) {
		if (con.get() == &context) {
			continue;
		}
		auto &context_lock = con->context_lock;
		client_locks.emplace_back(context_lock, std::move(con));
	}
	is_locking = false;
}

} // namespace duckdb
