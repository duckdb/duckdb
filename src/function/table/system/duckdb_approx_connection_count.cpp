#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection_manager.hpp"

namespace duckdb {

struct DuckDBApproxConnectionCountData : public GlobalTableFunctionState {
	DuckDBApproxConnectionCountData() : count(0), finished(false) {
	}
	idx_t count;
	bool finished;
};

static unique_ptr<FunctionData> DuckDBApproxConnectionCountBind(ClientContext &context, TableFunctionBindInput &input,
                                                                vector<LogicalType> &return_types,
                                                                vector<string> &names) {
	names.emplace_back("approx_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBApproxConnectionCountInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBApproxConnectionCountData>();
	auto &conn_manager = context.db->GetConnectionManager();
	result->count = conn_manager.GetConnectionCount();
	return std::move(result);
}

void DuckDBApproxConnectionCountFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBApproxConnectionCountData>();
	if (data.finished) {
		return;
	}
	output.SetValue(0, 0, Value::UBIGINT(data.count));
	output.SetCardinality(1);
	data.finished = true;
}

void DuckDBApproxConnectionCountFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_approx_connection_count", {}, DuckDBApproxConnectionCountFunction,
	                              DuckDBApproxConnectionCountBind, DuckDBApproxConnectionCountInit));
}

} // namespace duckdb
