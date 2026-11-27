#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>

namespace duckdb {

struct DuckDBLogSinksEntry {
	string name;
	string type;
	//! Whether the sink receives log entries
	bool active;
	string level;
	//! The log types the sink receives, NULL for every log type
	Value log_types;
	//! The options the user set on the sink, sorted by name
	InsertionOrderPreservingMap<string> config;
};

struct DuckDBLogSinksData : public GlobalTableFunctionState {
	//! The default log sink first, then the named log sinks by name
	vector<DuckDBLogSinksEntry> sinks;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckDBLogSinksBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("active");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("level");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("log_types");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("config");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	return nullptr;
}

static DuckDBLogSinksEntry CreateLogSinksEntry(const string &name, const LogSinkEntry &sink) {
	DuckDBLogSinksEntry result;
	result.name = name;
	result.type = sink.type;
	result.active = sink.filter.enabled;
	result.level = EnumUtil::ToString(sink.filter.level);
	result.log_types = Value(LogicalType::LIST(LogicalType::VARCHAR));
	if (sink.filter.mode == LogMode::ENABLE_SELECTED) {
		vector<string> log_types(sink.filter.enabled_log_types.begin(), sink.filter.enabled_log_types.end());
		std::sort(log_types.begin(), log_types.end());
		vector<Value> values;
		for (auto &log_type : log_types) {
			values.emplace_back(log_type);
		}
		result.log_types = Value::LIST(LogicalType::VARCHAR, std::move(values));
	}
	vector<string> keys;
	for (auto &entry : sink.config) {
		keys.push_back(entry.first);
	}
	std::sort(keys.begin(), keys.end());
	for (auto &key : keys) {
		result.config[key] = sink.config.at(key).ToString();
	}
	return result;
}

static unique_ptr<GlobalTableFunctionState> DuckDBLogSinksInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBLogSinksData>();
	auto sinks = LogManager::Get(context).GetConfigAndSinks();
	vector<string> names;
	for (auto &entry : sinks.named_log_sinks) {
		names.push_back(entry.first);
	}
	std::sort(names.begin(), names.end());

	result->sinks.push_back(CreateLogSinksEntry(LogConfig::DEFAULT_LOG_SINK_NAME, sinks.default_log_sink));
	for (auto &name : names) {
		result->sinks.push_back(CreateLogSinksEntry(name, sinks.named_log_sinks[name]));
	}
	return std::move(result);
}

static void DuckDBLogSinksFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBLogSinksData>();
	idx_t count = 0;
	while (data.offset < data.sinks.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.sinks[data.offset++];
		output.data[0].Append(Value(entry.name));
		output.data[1].Append(Value(entry.type));
		output.data[2].Append(Value::BOOLEAN(entry.active));
		output.data[3].Append(Value(entry.level));
		output.data[4].Append(entry.log_types);
		output.data[5].Append(Value::MAP(entry.config));
		count++;
	}
}

void DuckDBLogSinksFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction fun("duckdb_log_sinks", {}, DuckDBLogSinksFunction, DuckDBLogSinksBind, DuckDBLogSinksInit);
	// the log sinks change with enable_log_sink and disable_log_sink
	fun.stability = FunctionStability::VOLATILE;
	set.AddFunction(fun);
}

} // namespace duckdb
