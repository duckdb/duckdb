#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

struct DuckDBSettingValue {
	string name;
	Value value;
	string description;
	string input_type;
	string scope;
	vector<Value> aliases;

	inline bool operator<(const DuckDBSettingValue &rhs) const {
		return name < rhs.name;
	};
};

struct DuckDBSettingsData : public GlobalTableFunctionState {
	DuckDBSettingsData() : offset(0) {
	}

	vector<DuckDBSettingValue> settings;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBSettingsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("input_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("scope");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("aliases");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBSettingsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBSettingsData>();

	unordered_map<idx_t, vector<Value>> aliases;
	for (idx_t i = 0; i < DBConfig::GetAliasCount(); i++) {
		auto alias = DBConfig::GetAliasByIndex(i);
		aliases[alias->option_index].emplace_back(alias->alias);
	}

	auto &config = DBConfig::GetConfig(context);
	auto options_count = DBConfig::GetOptionCount();
	for (idx_t i = 0; i < options_count; i++) {
		auto option = DBConfig::GetOptionByIndex(i);
		D_ASSERT(option);
		DuckDBSettingValue value;
		auto scope = option->set_global ? SettingScope::GLOBAL : SettingScope::LOCAL;
		value.name = option->name;
		if (option->get_setting) {
			value.value = option->get_setting(context);
		} else {
			auto lookup_result = context.TryGetCurrentSetting(value.name, value.value);
			if (lookup_result) {
				scope = lookup_result.GetScope();
			} else {
				value.value = option->default_value;
			}
		}
		value.description = option->description;
		value.input_type = option->parameter_type;
		value.scope = EnumUtil::ToString(scope);
		auto entry = aliases.find(i);
		if (entry != aliases.end()) {
			value.aliases = std::move(entry->second);
		}
		for (auto &alias : value.aliases) {
			DuckDBSettingValue alias_value = value;
			alias_value.name = StringValue::Get(alias);
			alias_value.aliases.clear();
			result->settings.push_back(std::move(alias_value));
		}
		result->settings.push_back(std::move(value));
	}
	for (auto &ext_param : config.GetExtensionSettings()) {
		Value setting_val;
		auto scope = SettingScope::GLOBAL;
		auto lookup_result = context.TryGetCurrentSetting(ext_param.first, setting_val);
		if (lookup_result) {
			scope = lookup_result.GetScope();
		}
		DuckDBSettingValue value;
		value.name = ext_param.first;
		value.value = std::move(setting_val);
		value.description = ext_param.second.description;
		value.input_type = ext_param.second.type.ToString();
		value.scope = EnumUtil::ToString(scope);

		result->settings.push_back(std::move(value));
	}
	std::sort(result->settings.begin(), result->settings.end());
	return std::move(result);
}

void DuckDBSettingsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBSettingsData>();
	if (data.offset >= data.settings.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.settings.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.settings[data.offset++];

		// return values:
		// name, LogicalType::VARCHAR
		output.SetValue(0, count, Value(entry.name));
		// value, LogicalType::VARCHAR
		output.SetValue(1, count, entry.value.CastAs(context, LogicalType::VARCHAR));
		// description, LogicalType::VARCHAR
		output.SetValue(2, count, Value(entry.description));
		// input_type, LogicalType::VARCHAR
		output.SetValue(3, count, Value(entry.input_type));
		// scope, LogicalType::VARCHAR
		output.SetValue(4, count, Value(entry.scope));
		// aliases, LogicalType::VARCHAR[]
		output.SetValue(5, count, Value::LIST(LogicalType::VARCHAR, std::move(entry.aliases)));
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBSettingsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_settings", {}, DuckDBSettingsFunction, DuckDBSettingsBind, DuckDBSettingsInit));
}

} // namespace duckdb
