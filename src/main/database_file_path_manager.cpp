#include "duckdb/main/database_file_path_manager.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

namespace duckdb {

void DatabaseFilePathManager::CheckPathConflict(const string &path, const string &name) const {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return;
	}

	lock_guard<mutex> path_lock(db_paths_lock);
	auto entry = db_paths_to_name.find(path);
	if (entry != db_paths_to_name.end()) {
		throw BinderException("Unique file handle conflict: Cannot attach \"%s\" - the database file \"%s\" is already "
		                      "attached by database \"%s\"",
		                      name, path, entry->second);
	}
}

idx_t DatabaseFilePathManager::ApproxDatabaseCount() const {
	lock_guard<mutex> path_lock(db_paths_lock);
	return db_paths_to_name.size();
}

void DatabaseFilePathManager::InsertDatabasePath(const string &path, const string &name) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return;
	}

	lock_guard<mutex> path_lock(db_paths_lock);
	auto entry = db_paths_to_name.emplace(path, name);
	if (!entry.second) {
		throw BinderException("Unique file handle conflict: Cannot attach \"%s\" - the database file \"%s\" is already "
		                      "attached by database \"%s\"",
		                      name, path, entry.first->second);
	}
}

void DatabaseFilePathManager::EraseDatabasePath(const string &path) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return;
	}
	lock_guard<mutex> path_lock(db_paths_lock);
	db_paths_to_name.erase(path);
}

} // namespace duckdb
