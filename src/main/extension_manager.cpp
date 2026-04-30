#include "duckdb/main/extension_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/logging/log_manager.hpp"

namespace duckdb {

ExtensionInfo::ExtensionInfo() : is_loaded(false) {
}

void ExtensionActiveLoad::FinishLoad(ExtensionInstallInfo &install_info) {
	info.is_loaded = true;
	info.install_info = make_uniq<ExtensionInstallInfo>(install_info);
	string final_extension_name = extension_name;

	if (!alias.empty()) {
		final_extension_name = alias;
	}

	for (auto &callback : ExtensionCallback::Iterate(db)) {
		callback->OnExtensionLoaded(db, final_extension_name);
	}

	if (!alias.empty()) {
		DUCKDB_LOG_INFO(db, alias);
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
	return Get(DatabaseInstance::GetDatabase(context));
}

optional_ptr<ExtensionInfo> ExtensionManager::GetExtensionInfo(const string &name) {
	auto extension_name = ExtensionHelper::GetExtensionName(name);

	lock_guard<mutex> guard(lock);
	auto entries = loaded_extensions_info.find(extension_name);
	if (entries == loaded_extensions_info.end()) {
		return nullptr;
	}

	return entries->second.get();
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

unique_ptr<ExtensionActiveLoad> ExtensionManager::BeginLoad(const ExtensionLoadOptions &options) {
	if (!options.alias.empty()) {
		if (loaded_extensions_info.find(options.alias) != loaded_extensions_info.end()) {
			auto &info = loaded_extensions_info[options.alias];
			throw InvalidInputException("Alias '%s' already exists for extension '%s'", options.alias,
			                            info->orig_ext_name);
		}
	}

	string extension_name;
	extension_name = ExtensionHelper::GetExtensionName(options.extension_name);
	string original_extension_name = extension_name;

	unique_lock<mutex> extension_list_lock(lock);
	optional_ptr<ExtensionInfo> info;
	auto entry = loaded_extensions_info.end();

	if (options.alias.empty()) {
		// no alias given, we look if the extension is already loaded by using the original name
		entry = loaded_extensions_info.find(extension_name);
	}

	if (entry == loaded_extensions_info.end()) {
		// we don't have an entry yet - create one
		auto extension_info = make_uniq<ExtensionInfo>();
		extension_info->orig_ext_name = original_extension_name;
		info = extension_info.get();

		if (!options.alias.empty()) {
			// if aliasing is used, we need to register the alias instead of extension name
			extension_name = options.alias;
			info->alias = extension_name;
		}
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
	auto result = make_uniq<ExtensionActiveLoad>(db, *info, original_extension_name, extension_name);

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
