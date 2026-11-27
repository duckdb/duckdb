#include "duckdb/main/database_file_path_manager.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"

namespace duckdb {

DatabasePathInfo::DatabasePathInfo(DatabaseManager &manager, const Identifier &name_p, AccessMode access_mode)
    : name(name_p), access_mode(access_mode) {
	Attach(manager, name);
}

bool DatabasePathInfo::IsAttachedIn(DatabaseManager &manager, const Identifier &alias) const {
	auto manager_entry = attached_databases.find(manager);
	if (manager_entry == attached_databases.end()) {
		return false;
	}
	return manager_entry->second.find(alias) != manager_entry->second.end();
}

void DatabasePathInfo::Attach(DatabaseManager &manager, const Identifier &alias) {
	attached_databases[manager][alias]++;
}

void DatabasePathInfo::Detach(DatabaseManager &manager, const Identifier &alias) {
	auto manager_entry = attached_databases.find(manager);
	if (manager_entry == attached_databases.end()) {
		return;
	}
	auto &aliases = manager_entry->second;
	auto alias_entry = aliases.find(alias);
	if (alias_entry == aliases.end()) {
		return;
	}
	if (--alias_entry->second == 0) {
		aliases.erase(alias_entry);
	}
	if (aliases.empty()) {
		attached_databases.erase(manager_entry);
	}
}

Identifier DatabasePathInfo::GetAttachedName(DatabaseManager &manager) const {
	auto manager_entry = attached_databases.find(manager);
	if (manager_entry != attached_databases.end() && !manager_entry->second.empty()) {
		return manager_entry->second.begin()->first;
	}
	if (!attached_databases.empty()) {
		return attached_databases.begin()->second.begin()->first;
	}
	return name;
}

idx_t DatabaseFilePathManager::ApproxDatabaseCount() const {
	lock_guard<mutex> path_lock(db_paths_lock);
	return db_paths.size();
}

InsertDatabasePathResult DatabaseFilePathManager::InsertDatabasePath(DatabaseManager &manager, const string &path,
                                                                     const Identifier &name,
                                                                     OnCreateConflict on_conflict,
                                                                     AttachOptions &options) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		throw InternalException("DatabaseFilePathManager::InsertDatabasePath - cannot insert in-memory database");
	}
	lock_guard<mutex> path_lock(db_paths_lock);
	auto entry = db_paths.emplace(path, DatabasePathInfo(manager, name, options.access_mode));
	if (!entry.second) {
		auto &existing = entry.first->second;
		bool attached_in_this_system = false;
		bool already_exists = false;
		if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			attached_in_this_system = existing.IsAttachedIn(manager, name);
			already_exists = attached_in_this_system || (existing.attached_databases.empty() && existing.name == name);
		}
		if (options.access_mode == AccessMode::READ_ONLY && existing.access_mode == AccessMode::READ_ONLY) {
			if (attached_in_this_system) {
				return InsertDatabasePathResult::ALREADY_EXISTS;
			}
			// all attaches are in read-only mode - there is no conflict, just increase the reference count
			existing.Attach(manager, name);
			existing.reference_count++;
		} else {
			if (already_exists) {
				if (attached_in_this_system) {
					return InsertDatabasePathResult::ALREADY_EXISTS;
				}
				throw ResourceInUseException(
				    "Unique file handle conflict: Cannot attach %s - the database file \"%s\" is in "
				    "the process of being detached",
				    name, path);
			}
			throw ResourceInUseException(
			    "Unique file handle conflict: Cannot attach %s - the database file \"%s\" is already "
			    "attached by database \"%s\"",
			    name, path, existing.GetAttachedName(manager));
		}
	}
	options.stored_database_path = make_uniq<StoredDatabasePath>(manager, *this, path, name);
	return InsertDatabasePathResult::SUCCESS;
}

bool DatabaseFilePathManager::IsDatabasePath(const string &path) const {
	lock_guard<mutex> path_lock(db_paths_lock);
	return db_paths.find(path) != db_paths.end();
}

void DatabaseFilePathManager::EraseDatabasePath(StoredDatabasePath &stored_path) {
	if (stored_path.path.empty() || stored_path.path == IN_MEMORY_PATH) {
		return;
	}
	lock_guard<mutex> path_lock(db_paths_lock);
	auto entry = db_paths.find(stored_path.path);
	if (entry == db_paths.end()) {
		return;
	}
	if (!stored_path.detached) {
		stored_path.detached = true;
		entry->second.Detach(stored_path.db_manager, stored_path.name);
	}
	if (entry->second.reference_count <= 1) {
		db_paths.erase(entry);
	} else {
		entry->second.reference_count--;
	}
}

void DatabaseFilePathManager::DetachDatabase(StoredDatabasePath &stored_path) {
	if (stored_path.path.empty() || stored_path.path == IN_MEMORY_PATH) {
		return;
	}
	lock_guard<mutex> path_lock(db_paths_lock);
	if (stored_path.detached) {
		return;
	}
	stored_path.detached = true;
	auto entry = db_paths.find(stored_path.path);
	if (entry != db_paths.end()) {
		entry->second.Detach(stored_path.db_manager, stored_path.name);
		entry->second.name = stored_path.name;
	}
}

void DatabaseFilePathManager::RenameDatabasePath(StoredDatabasePath &stored_path, const Identifier &new_name) {
	if (stored_path.path.empty() || stored_path.path == IN_MEMORY_PATH) {
		return;
	}
	lock_guard<mutex> path_lock(db_paths_lock);
	if (stored_path.detached) {
		return;
	}
	auto entry = db_paths.find(stored_path.path);
	if (entry != db_paths.end()) {
		entry->second.Detach(stored_path.db_manager, stored_path.name);
		entry->second.Attach(stored_path.db_manager, new_name);
		if (entry->second.name == stored_path.name) {
			entry->second.name = new_name;
		}
	}
	stored_path.name = new_name;
}

} // namespace duckdb
