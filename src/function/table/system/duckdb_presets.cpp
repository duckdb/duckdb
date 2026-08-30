#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/setting_preset.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// duckdb_presets() - list the available presets
//===--------------------------------------------------------------------===//

struct DuckDBPresetsData : public GlobalTableFunctionState {
	DuckDBPresetsData() : offset(0) {
	}
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
	return make_uniq<DuckDBPresetsData>();
}

static void DuckDBPresetsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBPresetsData>();
	idx_t count = 0;
	while (data.offset < PresetRegistry::GetBuiltinCount() && count < STANDARD_VECTOR_SIZE) {
		auto &preset = PresetRegistry::GetBuiltinByIndex(data.offset++);

		vector<Value> settings;
		for (idx_t i = 0; i < preset.member_count; i++) {
			settings.emplace_back(preset.members[i].setting);
		}
		output.SetValue(0, count, Value(preset.name));
		output.SetValue(1, count, Value(preset.description));
		output.SetValue(2, count, Value::LIST(LogicalType::VARCHAR, std::move(settings)));
		output.SetValue(3, count, Value("builtin"));
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
	//! The name of a preset to look up, or empty when loading from a file
	string name;
	//! The path of a preset file, or empty when looking up by name
	string file;
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
	if (!input.inputs.empty() && !input.inputs[0].IsNull()) {
		result->name = input.inputs[0].ToString();
	}
	for (auto &entry : input.named_parameters) {
		if (entry.second.IsNull()) {
			continue;
		}
		if (entry.first == "file") {
			result->file = entry.second.ToString();
		} else if (entry.first == "name") {
			result->name = entry.second.ToString();
		}
	}
	if (result->name.empty() && result->file.empty()) {
		throw BinderException("preset requires either a preset name or file := '<path>'");
	}
	if (!result->name.empty() && !result->file.empty()) {
		throw BinderException("preset takes either a preset name or file := '<path>', not both");
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> PresetInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PresetBindData>();
	auto result = make_uniq<PresetApplyData>();
	auto preset = bind_data.file.empty() ? PresetRegistry::Resolve(context, bind_data.name)
	                                     : PresetRegistry::LoadFromFile(context, bind_data.file);
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
	TableFunctionSet functions("preset");
	// preset('name'), and preset(name := ...) / preset(file := ...)
	vector<vector<LogicalType>> signatures;
	signatures.emplace_back();
	signatures.push_back({LogicalType::VARCHAR});
	for (auto &arguments : signatures) {
		TableFunction function(arguments, PresetFunction, PresetBind, PresetInit);
		function.named_parameters["name"] = LogicalType::VARCHAR;
		function.named_parameters["file"] = LogicalType::VARCHAR;
		functions.AddFunction(std::move(function));
	}
	set.AddFunction(functions);
}

} // namespace duckdb
