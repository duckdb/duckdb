#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"

namespace duckdb {

struct DuckDBOptimizersData : public GlobalTableFunctionState {
	DuckDBOptimizersData() : offset(0) {
	}

	vector<string> optimizers;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBOptimizersBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBOptimizersInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBOptimizersData>();
	result->optimizers = ListAllOptimizers();
	return std::move(result);
}

void DuckDBOptimizersFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBOptimizersData>();
	if (data.offset >= data.optimizers.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.optimizers.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.optimizers[data.offset++];

		// return values:
		// name, LogicalType::VARCHAR
		output.SetValue(0, count, Value(entry));
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBOptimizersFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_optimizers", {}, DuckDBOptimizersFunction, DuckDBOptimizersBind, DuckDBOptimizersInit));
}

} // namespace duckdb
