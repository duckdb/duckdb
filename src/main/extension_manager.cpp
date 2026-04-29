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
		ExtensionManager::Get(db).AddExternalExtensionAlias(alias, extension_name);
	}

	for (auto &callback : ExtensionCallback::Iterate(db)) {
		callback->OnExtensionLoaded(db, final_extension_name);
	}

	if (!alias.empty()) {
		DUCKDB_LOG_INFO(db, final_extension_name);
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

void ExtensionManager::AddExternalExtensionAliasInternal(const string &alias, const string &extension_name) {
	// check if alias already there
	auto opt_extension_name = external_aliases.find(alias);

	if (opt_extension_name != external_aliases.end()) {
		throw InvalidInputException("Alias '%s' already exists for extension '%s' ", alias, opt_extension_name->second);
	}

	external_aliases[StringUtil::Lower(alias)] = StringUtil::Lower(extension_name);
}

void ExtensionManager::AddExternalExtensionAlias(const string &alias, const string &extension_name) {
	lock_guard<mutex> guard(lock);
	AddExternalExtensionAliasInternal(alias, extension_name);
}

string ExtensionManager::GetExternalExtensionName(const string &alias) {
	lock_guard<mutex> guard(lock);
	auto entry = external_aliases.find(alias);
	if (entry == external_aliases.end()) {
		return string();
	}
	return entry->second;
}

// bool ExtensionManager::GetExtensionEntry() {
//
// }

unique_ptr<ExtensionActiveLoad> ExtensionManager::BeginLoad(const ExtensionLoadOptions &options) {

	if (!options.alias.empty()) {
		if (external_aliases.find(options.alias) != external_aliases.end()) {
			auto original_extension_name = external_aliases[options.alias];
			throw InvalidInputException("Alias '%s' already exists for extension '%s'", options.alias, original_extension_name);
		}
	}

	string extension_name;
	extension_name = ExtensionHelper::GetExtensionName(options.extension_name);
	string original_extension_name = extension_name;

	unique_lock<mutex> extension_list_lock(lock);

	optional_ptr<ExtensionInfo> info;

	auto entry = loaded_extensions_info.end();

	if (options.extension_name.empty()) {
		// no alias given, we look if the extension is already loaded
		entry = loaded_extensions_info.find(extension_name);
	}

	// no entry found
	if (entry == loaded_extensions_info.end()) {
		// we don't have an entry yet - create one
		auto extension_info = make_uniq<ExtensionInfo>();
		info = extension_info.get();
		if (!options.alias.empty()) {
			// if aliasing is used, we need to register the alias instead of extension name
			extension_name = options.alias;
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

	// we now have a lock for loading the extension
	// HOWEVER - another thread might have finished loading in the meantime - double check to avoid a double load
	if (info->is_loaded) {
		//AddExternalExtensionAliasInternal(options.alias, extension_name);
		return nullptr;
	}
	for (auto &callback : ExtensionCallback::Iterate(db)) {
		callback->OnBeginExtensionLoad(db, extension_name);
	}
	// extension is not loaded yet and we are in charge of loading it - return
	return result;
}

} // namespace duckdb
