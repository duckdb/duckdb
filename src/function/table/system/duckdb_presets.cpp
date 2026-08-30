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
	while (data.offset < PresetRegistry::GetPresetCount() && count < STANDARD_VECTOR_SIZE) {
		auto &preset = PresetRegistry::GetPresetByIndex(data.offset++);

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
	vector<PresetMemberResult> results;
	idx_t offset;
};

struct PresetBindData : public TableFunctionData {
	explicit PresetBindData(string name_p) : name(std::move(name_p)) {
	}
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

	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("preset requires the name of a preset");
	}
	return make_uniq<PresetBindData>(input.inputs[0].ToString());
}

static unique_ptr<GlobalTableFunctionState> PresetInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PresetBindData>();
	auto result = make_uniq<PresetApplyData>();

	auto preset = PresetRegistry::Find(bind_data.name);
	if (!preset) {
		// there are few enough presets that listing them is more useful than a fuzzy guess
		vector<string> candidates;
		for (idx_t i = 0; i < PresetRegistry::GetPresetCount(); i++) {
			candidates.emplace_back(PresetRegistry::GetPresetByIndex(i).name);
		}
		throw CatalogException("Preset \"%s\" does not exist\nAvailable presets: %s", bind_data.name,
		                       StringUtil::Join(candidates, ", "));
	}
	result->results = PresetRegistry::Apply(context, *preset);
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
	set.AddFunction(
	    TableFunction("preset", {LogicalType::VARCHAR}, PresetFunction, PresetBind, PresetInit));
}

} // namespace duckdb
