#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/setting_preset.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// duckdb_presets() - list the available presets
//===--------------------------------------------------------------------===//

struct PresetListEntry {
	string name;
	string description;
	vector<Value> settings;
	string source;
};

struct DuckDBPresetsData : public GlobalTableFunctionState {
	DuckDBPresetsData() : offset(0) {
	}
	vector<PresetListEntry> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBPresetsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("settings");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("source");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBPresetsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBPresetsData>();
	auto &config = DBConfig::GetConfig(context);

	for (idx_t i = 0; i < PresetRegistry::GetBuiltinCount(); i++) {
		auto &preset = PresetRegistry::GetBuiltinByIndex(i);
		PresetListEntry entry;
		entry.name = preset.name;
		entry.description = preset.description;
		for (idx_t m = 0; m < preset.member_count; m++) {
			entry.settings.emplace_back(preset.members[m].setting);
		}
		entry.source = "builtin";
		result->entries.push_back(std::move(entry));
	}
	for (auto &preset : config.preset_manager->All()) {
		PresetListEntry entry;
		entry.name = preset.name;
		entry.description = preset.description;
		for (auto &setting : preset.settings) {
			entry.settings.emplace_back(setting.name);
		}
		entry.source = "session";
		result->entries.push_back(std::move(entry));
	}
	// the preset directory is the one place a listing has to enumerate; resolving a name by itself
	// never scans it
	auto &fs = FileSystem::GetFileSystem(context);
	auto directory = config.preset_manager->GetDirectory();
	if (fs.DirectoryExists(directory)) {
		fs.ListFiles(directory, [&](const string &name, bool is_directory) {
			auto add = [&](const string &preset_name) {
				PresetListEntry entry;
				entry.name = preset_name;
				entry.source = "file";
				result->entries.push_back(std::move(entry));
			};
			if (is_directory) {
				auto nested = fs.JoinPath(directory, name);
				fs.ListFiles(nested, [&](const string &child, bool child_is_directory) {
					if (!child_is_directory && StringUtil::EndsWith(child, ".json")) {
						add(name + ":" + child.substr(0, child.size() - 5));
					}
				});
			} else if (StringUtil::EndsWith(name, ".json")) {
				add(name.substr(0, name.size() - 5));
			}
		});
	}
	return std::move(result);
}

static void DuckDBPresetsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBPresetsData>();
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];
		output.SetValue(0, count, Value(entry.name));
		output.SetValue(1, count, entry.description.empty() ? Value(LogicalType::VARCHAR)
		                                                    : Value(entry.description));
		output.SetValue(2, count, Value::LIST(LogicalType::VARCHAR, entry.settings));
		output.SetValue(3, count, Value(entry.source));
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBPresetsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_presets", {}, DuckDBPresetsFunction, DuckDBPresetsBind, DuckDBPresetsInit));
}

//===--------------------------------------------------------------------===//
// preset(name) - apply a preset
//===--------------------------------------------------------------------===//

struct PresetApplyData : public GlobalTableFunctionState {
	PresetApplyData() : offset(0) {
	}
	vector<PresetApplyResult> results;
	idx_t offset;
};

struct PresetBindData : public TableFunctionData {
	//! A preset name, or the path of a preset file
	string name;
};

static unique_ptr<FunctionData> PresetBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("setting");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("old_value");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("new_value");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("note");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto result = make_uniq<PresetBindData>();
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("preset requires the name of a preset, or the path of a preset file");
	}
	result->name = input.inputs[0].ToString();
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> PresetInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PresetBindData>();
	auto result = make_uniq<PresetApplyData>();
	auto preset = PresetRegistry::Resolve(context, bind_data.name);
	result->results = PresetRegistry::Apply(context, preset);
	return std::move(result);
}

static void PresetFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PresetApplyData>();
	idx_t count = 0;
	while (data.offset < data.results.size() && count < STANDARD_VECTOR_SIZE) {
		auto &result = data.results[data.offset++];
		output.SetValue(0, count, Value(result.setting));
		output.SetValue(1, count, result.old_value.IsNull() ? Value(LogicalType::VARCHAR)
		                                                    : Value(result.old_value.ToString()));
		output.SetValue(2, count, result.new_value.IsNull() ? Value(LogicalType::VARCHAR)
		                                                    : Value(result.new_value.ToString()));
		output.SetValue(3, count, Value(result.status));
		output.SetValue(4, count, result.note.empty() ? Value(LogicalType::VARCHAR) : Value(result.note));
		count++;
	}
	output.SetCardinality(count);
}

void PresetFun::RegisterFunction(BuiltinFunctions &set) {
	// one argument: a preset name, or the path of a preset file. Names may not contain a dot,
	// slash or backslash, so the two can always be told apart.
	set.AddFunction(TableFunction("preset", {LogicalType::VARCHAR}, PresetFunction, PresetBind, PresetInit));
}


//===--------------------------------------------------------------------===//
// register_preset(name, {settings}) - define a preset
//===--------------------------------------------------------------------===//

struct RegisterPresetData : public GlobalTableFunctionState {
	RegisterPresetData() : offset(0) {
	}
	Preset preset;
	bool persistent = false;
	idx_t offset;
};

struct RegisterPresetBindData : public TableFunctionData {
	Preset preset;
	bool persistent = false;
};

static unique_ptr<FunctionData> RegisterPresetBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("setting");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("source");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto result = make_uniq<RegisterPresetBindData>();
	if (input.inputs.size() < 2 || input.inputs[0].IsNull()) {
		throw BinderException("register_preset requires a name and a struct of settings");
	}
	result->preset.name = input.inputs[0].ToString();
	if (ExtensionHelper::IsFullPath(result->preset.name)) {
		// such a name would be read back as a file path and could never be resolved
		throw BinderException("Preset name \"%s\" may not contain a dot, slash or backslash",
		                      result->preset.name);
	}
	result->preset.source = "session";

	auto &settings = input.inputs[1];
	if (settings.type().id() != LogicalTypeId::STRUCT) {
		throw BinderException("register_preset expects the settings as a struct, e.g. {'threads': 4}");
	}
	// a struct preserves the order its fields were written in, which is what decides the order the
	// settings are applied in
	auto &children = StructType::GetChildTypes(settings.type());
	auto &values = StructValue::GetChildren(settings);
	for (idx_t i = 0; i < children.size(); i++) {
		PresetSetting setting;
		setting.name = children[i].first.GetIdentifierName();
		if (values[i].IsNull()) {
			setting.is_reset = true;
		} else {
			setting.value = values[i];
		}
		result->preset.settings.push_back(std::move(setting));
	}
	if (result->preset.settings.empty()) {
		throw BinderException("register_preset requires at least one setting");
	}
	for (auto &entry : input.named_parameters) {
		if (entry.second.IsNull()) {
			continue;
		}
		if (entry.first == "persistent") {
			result->persistent = BooleanValue::Get(entry.second);
		} else if (entry.first == "description") {
			result->preset.description = entry.second.ToString();
		}
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RegisterPresetInit(ClientContext &context,
                                                               TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RegisterPresetBindData>();
	auto result = make_uniq<RegisterPresetData>();
	result->preset = bind_data.preset;
	result->persistent = bind_data.persistent;

	auto &config = DBConfig::GetConfig(context);
	config.preset_manager->Register(result->preset);
	if (result->persistent) {
		// nothing is written to disk unless persistence is asked for, as with a secret
		PresetRegistry::Persist(context, result->preset);
	}
	return std::move(result);
}

static void RegisterPresetFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<RegisterPresetData>();
	idx_t count = 0;
	while (data.offset < data.preset.settings.size() && count < STANDARD_VECTOR_SIZE) {
		auto &setting = data.preset.settings[data.offset++];
		output.SetValue(0, count, Value(setting.name));
		output.SetValue(1, count, setting.is_reset ? Value(LogicalType::VARCHAR) : Value(setting.value.ToString()));
		output.SetValue(2, count, Value(data.persistent ? "file" : "session"));
		count++;
	}
	output.SetCardinality(count);
}

void RegisterPresetFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction function("register_preset", {LogicalType::VARCHAR, LogicalType::ANY}, RegisterPresetFunction,
	                       RegisterPresetBind, RegisterPresetInit);
	function.named_parameters["persistent"] = LogicalType::BOOLEAN;
	function.named_parameters["description"] = LogicalType::VARCHAR;
	set.AddFunction(function);
}

} // namespace duckdb
