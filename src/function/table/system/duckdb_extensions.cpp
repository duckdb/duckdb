#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

struct ExtensionInformation {
	string name;
	bool loaded = false;
	bool installed = false;
	string file_path;
	string description;
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

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBExtensionsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<DuckDBExtensionsData>();

	auto &fs = FileSystem::GetFileSystem(context);
	auto &db = DatabaseInstance::GetDatabase(context);

	map<string, ExtensionInformation> installed_extensions;
	auto extension_count = ExtensionHelper::DefaultExtensionCount();
	for (idx_t i = 0; i < extension_count; i++) {
		auto extension = ExtensionHelper::GetDefaultExtension(i);
		ExtensionInformation info;
		info.name = extension.name;
		info.installed = extension.statically_loaded;
		info.loaded = false;
		info.file_path = extension.statically_loaded ? "(BUILT-IN)" : string();
		info.description = extension.description;
		installed_extensions[info.name] = move(info);
	}

	// scan the install directory for installed extensions
	auto ext_directory = ExtensionHelper::ExtensionDirectory(fs);
	fs.ListFiles(ext_directory, [&](const string &path, bool is_directory) {
		if (!StringUtil::EndsWith(path, ".duckdb_extension")) {
			return;
		}
		ExtensionInformation info;
		info.name = fs.ExtractBaseName(path);
		info.loaded = false;
		info.file_path = fs.JoinPath(ext_directory, path);
		auto entry = installed_extensions.find(info.name);
		if (entry == installed_extensions.end()) {
			installed_extensions[info.name] = move(info);
		} else {
			if (!entry->second.loaded) {
				entry->second.file_path = info.file_path;
			}
			entry->second.installed = true;
		}
	});

	// now check the list of currently loaded extensions
	auto &loaded_extensions = db.LoadedExtensions();
	for (auto &ext_name : loaded_extensions) {
		auto entry = installed_extensions.find(ext_name);
		if (entry == installed_extensions.end()) {
			ExtensionInformation info;
			info.name = ext_name;
			info.loaded = true;
			installed_extensions[ext_name] = move(info);
		} else {
			entry->second.loaded = true;
		}
	}

	result->entries.reserve(installed_extensions.size());
	for (auto &kv : installed_extensions) {
		result->entries.push_back(move(kv.second));
	}
	return move(result);
}

void DuckDBExtensionsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DuckDBExtensionsData &)*data_p.global_state;
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
		output.SetValue(2, count, !entry.installed && entry.loaded ? Value() : Value::BOOLEAN(entry.installed));
		// install_path LogicalType::VARCHAR
		output.SetValue(3, count, Value(entry.file_path));
		// description LogicalType::VARCHAR
		output.SetValue(4, count, Value(entry.description));

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
