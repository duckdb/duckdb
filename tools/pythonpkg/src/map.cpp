#include "duckdb_python/map.hpp"

namespace duckdb {

MapFunction::MapFunction()
    : TableFunction("python_map_function", {LogicalType::TABLE, LogicalType::POINTER}, MapFunctionExec,
                    MapFunctionBind) {
}

struct MapFunctionData : public TableFunctionData {
	MapFunctionData() {
	}
};

unique_ptr<FunctionData> MapFunction::MapFunctionBind(ClientContext &context, vector<Value> &inputs,
                                                      unordered_map<string, Value> &named_parameters,
                                                      vector<LogicalType> &input_table_types,
                                                      vector<string> &input_table_names,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	py::gil_scoped_acquire acquire;

	for (idx_t col_idx = 0; col_idx < input_table_types.size(); col_idx++) {
		return_types.emplace_back(input_table_types[col_idx]);
		names.emplace_back(input_table_names[col_idx]);
	}

	return make_unique<MapFunctionData>();
}

void MapFunction::MapFunctionExec(ClientContext &context, const FunctionData *bind_data,
                                  FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {

	py::gil_scoped_acquire acquire;
	auto &data = (MapFunctionData &)*bind_data;
	output.Reference(*input);
}

} // namespace duckdb
