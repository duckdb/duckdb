//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/system/duckdb_wait_events.cpp
//
// Exposes duckdb_wait_events() table function
//===----------------------------------------------------------------------===//

#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/common/wait_events.hpp"

namespace duckdb {

struct DuckDBWaitEventsGlobalState : public GlobalTableFunctionState {
	explicit DuckDBWaitEventsGlobalState(vector<WaitEventRecord> recs) : records(std::move(recs)) {
	}
	vector<WaitEventRecord> records;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> WaitEventsBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("connection_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("thread_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("query_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("query");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("event_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("duration_us");
	return_types.emplace_back(LogicalType::UBIGINT);

	// additional duration representations
	names.emplace_back("duration_ms");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("duration_s");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("metadata");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> WaitEventsInit(ClientContext &context, TableFunctionInitInput &input) {
	// TODO - try implementing a parameter to collect from all connections (maybe it should even be the default?)
	auto records = WaitEvents::CollectForCurrentConnection(context);
	return make_uniq<DuckDBWaitEventsGlobalState>(std::move(records));
}

static void WaitEventsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DuckDBWaitEventsGlobalState>();
	idx_t count = 0;
	while (state.offset < state.records.size() && count < STANDARD_VECTOR_SIZE) {
		auto &rec = state.records[state.offset++];
		output.SetValue(0, count, Value::UBIGINT(rec.connection_id));
		output.SetValue(1, count, Value::UBIGINT(rec.thread_id));

		if (rec.query_id.IsValid()) {
			output.SetValue(2, count, Value::UBIGINT(rec.query_id.GetIndex()));
			output.SetValue(3, count, Value(rec.query_string));
		} else {
			output.SetValue(2, count, Value());
			output.SetValue(3, count, Value());
		}

		output.SetValue(4, count, Value(WaitEvents::TypeToString(rec.type)));
		output.SetValue(5, count, Value::UBIGINT(rec.duration_us));
		// convert to milliseconds and seconds (double)
		const double duration_ms = static_cast<double>(rec.duration_us) / 1000.0;
		const double duration_s = static_cast<double>(rec.duration_us) / 1000000.0;
		output.SetValue(6, count, Value::DOUBLE(duration_ms));
		output.SetValue(7, count, Value::DOUBLE(duration_s));

		output.SetValue(8, count, Value(rec.metadata));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBWaitEventsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_wait_events", {}, WaitEventsFunc, WaitEventsBind, WaitEventsInit));
}

} // namespace duckdb
