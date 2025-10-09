#include "duckdb/main/database_file_path_manager.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

idx_t DatabaseFilePathManager::ApproxDatabaseCount() const {
	lock_guard<mutex> path_lock(db_paths_lock);
	return db_paths.size();
}

InsertDatabasePathResult DatabaseFilePathManager::InsertDatabasePath(const string &path, const string &name,
                                                                     OnCreateConflict on_conflict,
                                                                     AttachOptions &options) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return InsertDatabasePathResult::SUCCESS;
	}

	lock_guard<mutex> path_lock(db_paths_lock);
	auto entry = db_paths.emplace(path, DatabasePathInfo(name, options.access_mode));
	if (!entry.second) {
		auto &existing = entry.first->second;
		if (options.access_mode == AccessMode::READ_ONLY && existing.access_mode == AccessMode::READ_ONLY) {
			// all attaches are in read-only mode - there is no conflict, just increase the reference count
			existing.reference_count++;
		} else {
			if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT && existing.name == name) {
				if (existing.is_attached) {
					return InsertDatabasePathResult::ALREADY_EXISTS;
				}
				throw BinderException(
				    "Unique file handle conflict: Cannot attach \"%s\" - the database file \"%s\" is in "
				    "the process of being detached",
				    name, path);
			}
			throw BinderException(
			    "Unique file handle conflict: Cannot attach \"%s\" - the database file \"%s\" is already "
			    "attached by database \"%s\"",
			    name, path, existing.name);
		}
	}
	options.stored_database_path = make_uniq<StoredDatabasePath>(*this, path, name);
	return InsertDatabasePathResult::SUCCESS;
}

void DatabaseFilePathManager::EraseDatabasePath(const string &path) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return;
	}
	lock_guard<mutex> path_lock(db_paths_lock);
	auto entry = db_paths.find(path);
	if (entry != db_paths.end()) {
		if (entry->second.reference_count <= 1) {
			db_paths.erase(entry);
		} else {
			entry->second.reference_count--;
		}
	}
}

void DatabaseFilePathManager::DetachDatabase(const string &path) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return;
	}
	lock_guard<mutex> path_lock(db_paths_lock);
	auto entry = db_paths.find(path);
	if (entry != db_paths.end()) {
		entry->second.is_attached = false;
	}
}

} // namespace duckdb
