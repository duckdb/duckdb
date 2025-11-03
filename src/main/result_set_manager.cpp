#include "duckdb/main/result_set_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

ManagedResultSet::ManagedResultSet() : valid(false) {
}

ManagedResultSet::ManagedResultSet(const weak_ptr<DatabaseInstance> &db_p, vector<shared_ptr<BlockHandle>> &handles_p)
    : valid(true), db(db_p), handles(handles_p) {
}

bool ManagedResultSet::IsValid() const {
	return valid;
}

shared_ptr<DatabaseInstance> ManagedResultSet::GetDatabase() const {
	D_ASSERT(IsValid());
	return db.lock();
}

vector<shared_ptr<BlockHandle>> &ManagedResultSet::GetHandles() {
	D_ASSERT(IsValid());
	return *handles;
}

ResultSetManager::ResultSetManager(DatabaseInstance &db_p) : db(db_p.shared_from_this()) {
}

ResultSetManager &ResultSetManager::Get(ClientContext &context) {
	return Get(*context.db);
}

ResultSetManager &ResultSetManager::Get(DatabaseInstance &db_p) {
	return db_p.GetResultSetManager();
}

ManagedResultSet ResultSetManager::Add(ColumnDataAllocator &allocator) {
	lock_guard<mutex> guard(lock);
	auto &handles = *open_results.emplace(allocator, make_uniq<vector<shared_ptr<BlockHandle>>>()).first->second;
	return ManagedResultSet(db, handles);
}

void ResultSetManager::Remove(ColumnDataAllocator &allocator) {
	lock_guard<mutex> guard(lock);
	open_results.erase(allocator);
}

} // namespace duckdb
