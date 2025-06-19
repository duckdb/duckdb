#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"

#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/main/extension_install_info.hpp"

namespace duckdb {

struct ExtensionInformation {
	string name;
	bool loaded = false;
	bool installed = false;
	string file_path;
	ExtensionInstallMode install_mode;
	string installed_from;
	string description;
	vector<Value> aliases;
	string extension_version;
};

struct DuckDBExtensionsData : public GlobalTableFunctionState {
	DuckDBExtensionsData() : offset(0) {
	}

	vector<ExtensionInformation> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBExtensionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("extension_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("loaded");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("installed");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("install_path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("aliases");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("extension_version");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("install_mode");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("installed_from");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBExtensionsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBExtensionsData>();

	auto &fs = FileSystem::GetFileSystem(context);
	auto &db = DatabaseInstance::GetDatabase(context);

	// Firstly, we go over all Default Extensions: duckdb_extensions always prints those, installed/loaded or not
	map<string, ExtensionInformation> installed_extensions;
	auto extension_count = ExtensionHelper::DefaultExtensionCount();
	auto alias_count = ExtensionHelper::ExtensionAliasCount();
	for (idx_t i = 0; i < extension_count; i++) {
		auto extension = ExtensionHelper::GetDefaultExtension(i);
		ExtensionInformation info;
		info.name = extension.name;
		info.installed = extension.statically_loaded;
		info.loaded = false;
		info.file_path = extension.statically_loaded ? "(BUILT-IN)" : string();
		info.install_mode =
		    extension.statically_loaded ? ExtensionInstallMode::STATICALLY_LINKED : ExtensionInstallMode::NOT_INSTALLED;
		info.description = extension.description;
		for (idx_t k = 0; k < alias_count; k++) {
			auto alias = ExtensionHelper::GetExtensionAlias(k);
			if (info.name == alias.extension) {
				info.aliases.emplace_back(alias.alias);
			}
		}
		installed_extensions[info.name] = std::move(info);
	}

	// Secondly we scan all installed extensions and their install info
#ifndef WASM_LOADABLE_EXTENSIONS
	auto ext_directory = ExtensionHelper::GetExtensionDirectoryPath(context);
	fs.ListFiles(ext_directory, [&](const string &path, bool is_directory) {
		if (!StringUtil::EndsWith(path, ".duckdb_extension")) {
			return;
		}
		ExtensionInformation info;
		info.name = fs.ExtractBaseName(path);
		info.installed = true;
		info.loaded = false;
		info.file_path = fs.JoinPath(ext_directory, path);

		// Check the info file for its installation source
		auto info_file_path = fs.JoinPath(ext_directory, path + ".info");

		// Read the info file
		auto extension_install_info = ExtensionInstallInfo::TryReadInfoFile(fs, info_file_path, info.name);
		info.install_mode = extension_install_info->mode;
		info.extension_version = extension_install_info->version;
		if (extension_install_info->mode == ExtensionInstallMode::REPOSITORY) {
			info.installed_from = ExtensionRepository::GetRepository(extension_install_info->repository_url);
		} else {
			info.installed_from = extension_install_info->full_path;
		}

		auto entry = installed_extensions.find(info.name);
		if (entry == installed_extensions.end()) {
			installed_extensions[info.name] = std::move(info);
		} else {
			if (entry->second.install_mode != ExtensionInstallMode::STATICALLY_LINKED) {
				entry->second.file_path = info.file_path;
				entry->second.install_mode = info.install_mode;
				entry->second.installed_from = info.installed_from;
				entry->second.install_mode = info.install_mode;
				entry->second.extension_version = info.extension_version;
			}
			entry->second.installed = true;
		}
	});
#endif

	// Finally, we check the list of currently loaded extensions
	auto &extensions = db.GetExtensions();
	for (auto &e : extensions) {
		if (!e.second.is_loaded) {
			continue;
		}
		auto &ext_name = e.first;
		auto &ext_data = e.second;
		if (auto &ext_install_info = ext_data.install_info) {
			auto entry = installed_extensions.find(ext_name);
			if (entry == installed_extensions.end() || !entry->second.installed) {
				ExtensionInformation &info = installed_extensions[ext_name];

				info.name = ext_name;
				info.loaded = true;
				info.extension_version = ext_install_info->version;
				info.installed = ext_install_info->mode == ExtensionInstallMode::STATICALLY_LINKED;
				info.install_mode = ext_install_info->mode;
				if (ext_data.install_info->mode == ExtensionInstallMode::STATICALLY_LINKED && info.file_path.empty()) {
					info.file_path = "(BUILT-IN)";
				}
			} else {
				entry->second.loaded = true;
				entry->second.extension_version = ext_install_info->version;
			}
		}
		if (auto &ext_load_info = ext_data.load_info) {
			auto entry = installed_extensions.find(ext_name);
			if (entry != installed_extensions.end()) {
				entry->second.description = ext_load_info->description;
			}
		}
	}

	result->entries.reserve(installed_extensions.size());
	for (auto &kv : installed_extensions) {
		result->entries.push_back(std::move(kv.second));
	}
	return std::move(result);
}

void DuckDBExtensionsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBExtensionsData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset];

		// return values:
		// extension_name LogicalType::VARCHAR
		output.SetValue(0, count, Value(entry.name));
		// loaded LogicalType::BOOLEAN
		output.SetValue(1, count, Value::BOOLEAN(entry.loaded));
		// installed LogicalType::BOOLEAN
		output.SetValue(2, count, Value::BOOLEAN(entry.installed));
		// install_path LogicalType::VARCHAR
		output.SetValue(3, count, Value(entry.file_path));
		// description LogicalType::VARCHAR
		output.SetValue(4, count, Value(entry.description));
		// aliases     LogicalType::LIST(LogicalType::VARCHAR)
		output.SetValue(5, count, Value::LIST(LogicalType::VARCHAR, entry.aliases));
		// extension version     LogicalType::LIST(LogicalType::VARCHAR)
		output.SetValue(6, count, Value(entry.extension_version));
		// installed_mode LogicalType::VARCHAR
		output.SetValue(7, count, EnumUtil::ToString(entry.install_mode));
		// installed_source LogicalType::VARCHAR
		output.SetValue(8, count, Value(entry.installed_from));

		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBExtensionsFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet functions("duckdb_extensions");
	functions.AddFunction(TableFunction({}, DuckDBExtensionsFunction, DuckDBExtensionsBind, DuckDBExtensionsInit));
	set.AddFunction(functions);
}

} // namespace duckdb
