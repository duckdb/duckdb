#include "duckdb/main/result_set_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

ManagedResultSet::ManagedResultSet(const weak_ptr<DatabaseInstance> &db_p) : db(db_p) {
}

ResultSetManager::ResultSetManager(DatabaseInstance &db_p) : db(db_p.shared_from_this()) {
}

ResultSetManager &ResultSetManager::Get(ClientContext &context) {
	return Get(*context.db);
}

ResultSetManager &ResultSetManager::Get(DatabaseInstance &db_p) {
	return db_p.GetResultSetManager();
}

optional_ptr<ManagedResultSet> ResultSetManager::Add(ColumnDataAllocator &allocator) {
	lock_guard<mutex> guard(lock);
	return open_results.emplace(allocator, make_uniq<ManagedResultSet>(db)).first->second.get();
}

void ResultSetManager::Remove(ColumnDataAllocator &allocator) {
	lock_guard<mutex> guard(lock);
	open_results.erase(allocator);
}

} // namespace duckdb
