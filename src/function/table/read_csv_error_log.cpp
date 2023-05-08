#include "duckdb/function/table/read_csv_error_log.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"


namespace duckdb {

struct State : public GlobalTableFunctionState {
    idx_t cursor = 0;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                    vector<LogicalType> &return_types, vector<string> &names) {

    return_types.push_back(LogicalType::BIGINT);
    names.push_back("line_number");

    return_types.push_back(LogicalType::BIGINT);
    names.push_back("column_number");

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("parsed_value");

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("error");

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("file_name");

    return nullptr;
}

unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<State>();
}

static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<State>();
    idx_t total = context.client_data->read_csv_error_log->errors.size();
    idx_t count = 0;
    auto next_idx = MinValue<idx_t>(state.cursor + STANDARD_VECTOR_SIZE, total);

    for (idx_t error_idx = state.cursor; error_idx < next_idx; error_idx++) {
        auto &err = context.client_data->read_csv_error_log->errors[error_idx];

        output.SetValue(0, count, Value::BIGINT(err.line));
        output.SetValue(1, count, Value::BIGINT(err.column));
        output.SetValue(2, count, Value(err.parsed_value));
        output.SetValue(3, count, Value(err.error));
        output.SetValue(4, count, Value(err.file_name));
        count++;
    }

    state.cursor += count;
    output.SetCardinality(count);
}

void ReadCSVErrorLogTableFunction::RegisterFunction(BuiltinFunctions &set) {
    set.AddFunction(TableFunction("read_csv_error_log", {}, Execute, Bind, Init));
}

}