#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/metrics_manager.hpp"

namespace duckdb {

struct DuckDBMetricsData : public GlobalTableFunctionState {
	DuckDBMetricsData() : offset(0) {
	}

	vector<MetricInfo> metrics;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBMetricsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("metric_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("metric_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("unit");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBMetricsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBMetricsData>();
	result->metrics = MetricsManager::Get(context).GetAllMetrics();
	return std::move(result);
}

void DuckDBMetricsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBMetricsData>();
	if (data.offset >= data.metrics.size()) {
		return;
	}

	idx_t count = 0;
	auto &metric_name = output.data[0];
	auto &metric_type = output.data[1];
	auto &description = output.data[2];
	auto &unit = output.data[3];

	while (data.offset < data.metrics.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.metrics[data.offset++];

		metric_name.Append(Value(entry.name));
		metric_type.Append(Value(entry.metric_type));
		description.Append(Value(entry.description));
		unit.Append(Value(entry.unit));
		count++;
	}
}

void DuckDBMetricsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_available_metrics", {}, DuckDBMetricsFunction, DuckDBMetricsBind, DuckDBMetricsInit));
}

} // namespace duckdb
