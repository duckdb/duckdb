#include "duckdb/main/extension_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/logging/log_manager.hpp"

namespace duckdb {

ExtensionInfo::ExtensionInfo() : is_loaded(false) {
}

ExtensionActiveLoad::ExtensionActiveLoad(DatabaseInstance &db, ExtensionInfo &info, string extension_name_p)
    : db(db), load_lock(info.lock), info(info), extension_name(std::move(extension_name_p)) {
}

void ExtensionActiveLoad::FinishLoad(ExtensionInstallInfo &install_info) {
	info.is_loaded = true;
	info.install_info = make_uniq<ExtensionInstallInfo>(install_info);

	for (auto &callback : ExtensionCallback::Iterate(db)) {
		callback->OnExtensionLoaded(db, extension_name);
	}
	DUCKDB_LOG_INFO(db, extension_name);
}

void ExtensionActiveLoad::LoadFail(const ErrorData &error) {
	for (auto &callback : ExtensionCallback::Iterate(db)) {
		callback->OnExtensionLoadFail(db, extension_name, error);
	}
	DUCKDB_LOG_INFO(db, "Failed to load extension '%s': %s", extension_name, error.Message());
}

ExtensionManager::ExtensionManager(DatabaseInstance &db) : db(db) {
}

ExtensionManager &ExtensionManager::Get(DatabaseInstance &db) {
	return db.GetExtensionManager();
}

ExtensionManager &ExtensionManager::Get(ClientContext &context) {
	return ExtensionManager::Get(DatabaseInstance::GetDatabase(context));
}

optional_ptr<ExtensionInfo> ExtensionManager::GetExtensionInfo(const string &name) {
	auto extension_name = ExtensionHelper::GetExtensionName(name);

	lock_guard<mutex> guard(lock);
	auto entry = loaded_extensions_info.find(extension_name);
	if (entry == loaded_extensions_info.end()) {
		return nullptr;
	}
	return entry->second.get();
}

vector<string> ExtensionManager::GetExtensions() {
	lock_guard<mutex> guard(lock);

	vector<string> result;
	for (auto &entry : loaded_extensions_info) {
		result.push_back(entry.first);
	}
	return result;
}

bool ExtensionManager::ExtensionIsLoaded(const string &name) {
	auto info = GetExtensionInfo(name);
	if (!info) {
		return false;
	}
	return info->is_loaded;
}

unique_ptr<ExtensionActiveLoad> ExtensionManager::BeginLoad(const string &name) {
	auto extension_name = ExtensionHelper::GetExtensionName(name);

	unique_lock<mutex> extension_list_lock(lock);

	optional_ptr<ExtensionInfo> info;
	auto entry = loaded_extensions_info.find(extension_name);
	if (entry == loaded_extensions_info.end()) {
		// we don't have an entry yet - create one
		auto extension_info = make_uniq<ExtensionInfo>();
		info = extension_info.get();
		loaded_extensions_info.emplace(extension_name, std::move(extension_info));
	} else {
		// we already have an entry
		if (entry->second->is_loaded) {
			// and it is loaded! we are done
			return nullptr;
		}
		// it is not loaded yet - try to load it
		info = entry->second.get();
	}
	extension_list_lock.unlock();

	// we have an extension and we want to try to load it - instantiate the load
	// we instantiate the ExtensionActiveLoad which also grabs the lock for loading the specific extension
	auto result = make_uniq<ExtensionActiveLoad>(db, *info, extension_name);

	// we now have a lock for loading the extension
	// HOWEVER - another thread might have finished loading in the meantime - double check to avoid a double load
	if (info->is_loaded) {
		return nullptr;
	}
	for (auto &callback : ExtensionCallback::Iterate(db)) {
		callback->OnBeginExtensionLoad(db, extension_name);
	}
	// extension is not loaded yet and we are in charge of loading it - return
	return result;
}

} // namespace duckdb
