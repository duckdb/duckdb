#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>

namespace duckdb {

struct DuckDBLogSinksData : public GlobalTableFunctionState {
	//! The main log sink first, then the added log sinks by name
	vector<string> sink_names;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckDBLogSinksBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("main");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBLogSinksInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBLogSinksData>();
	auto sinks = LogManager::Get(context).GetConfigAndSinks();
	vector<string> added_sinks;
	for (auto &entry : sinks.added_log_sinks) {
		added_sinks.push_back(entry.first);
	}
	std::sort(added_sinks.begin(), added_sinks.end());

	result->sink_names.push_back(sinks.config.storage);
	result->sink_names.insert(result->sink_names.end(), added_sinks.begin(), added_sinks.end());
	return std::move(result);
}

static void DuckDBLogSinksFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBLogSinksData>();
	if (data.offset >= data.sink_names.size()) {
		return;
	}
	auto count = MinValue<idx_t>(data.sink_names.size() - data.offset, STANDARD_VECTOR_SIZE);
	auto name_writer = FlatVector::Writer<string_t>(output.data[0], count);
	auto main_writer = FlatVector::Writer<bool>(output.data[1], count);
	for (idx_t i = 0; i < count; i++) {
		auto row = data.offset++;
		name_writer.WriteValue(string_t(data.sink_names[row]));
		main_writer.WriteValue(row == 0);
	}
}

void DuckDBLogSinksFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction fun("duckdb_log_sinks", {}, DuckDBLogSinksFunction, DuckDBLogSinksBind, DuckDBLogSinksInit);
	// the log sinks change with add_log_sink and remove_log_sink
	fun.stability = FunctionStability::VOLATILE;
	set.AddFunction(fun);
}

} // namespace duckdb
