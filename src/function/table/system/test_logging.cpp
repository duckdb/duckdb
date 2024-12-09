#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

struct TestLoggingBindData : public TableFunctionData {
	idx_t total_tuples = 10;
	bool logging_enabled = true;
	string log_message = "test_logging test message";
	string logger = "client_context";
};

struct TestLoggingData : public LocalTableFunctionState {
	explicit TestLoggingData(ExecutionContext &context_p) : context(context_p), current_tuple(0) {
	};
	ExecutionContext &context;
	idx_t current_tuple;
};

static unique_ptr<FunctionData> TestLoggingBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto res = make_uniq<TestLoggingBindData>();

	res->total_tuples = input.inputs[0].GetValue<idx_t>();

	for (const auto &param : input.named_parameters) {
		auto param_name = StringUtil::Lower(param.first);
		if (param_name == "enable_logging") {
			res->logging_enabled = param.second.GetValue<bool>();
		} else if (param_name == "log_message") {
			res->log_message = param.second.GetValue<string>();
		} else if (param_name == "logger") {
			res->logger = param.second.GetValue<string>();
		} else {
			throw InvalidInputException("Unknown input passed: '%s'", param_name);
		}
	}

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::UBIGINT);

	return std::move(res);
}

template <bool LOG, class LOGGER>
static void InnerLogTestLoop(TestLoggingData& data, const TestLoggingBindData &bind_data, idx_t tuples_to_create, DataChunk &output, LOGGER &source) {
	for (idx_t i = 0; i < tuples_to_create; i++) {
		if(LOG) {
			Logger::Info(source, bind_data.log_message.c_str());
		}
		FlatVector::GetData<idx_t>(output.data[0])[i] = data.current_tuple++;
	}
	output.SetCardinality(tuples_to_create);
}

template <bool LOG>
static void LogTest(TestLoggingData& data, const TestLoggingBindData &bind_data, idx_t tuples_to_create, DataChunk &output, ClientContext &context, const string &logger) {
	if (logger == "global") {
		InnerLogTestLoop<LOG>(data,bind_data, tuples_to_create, output, *context.db);
	} else if (logger == "client_context") {
		InnerLogTestLoop<LOG>(data,bind_data, tuples_to_create, output, context);
	} else if (logger == "thread_local") {
		InnerLogTestLoop<LOG>(data,bind_data, tuples_to_create, output, data.context);
	} else if (logger == "file_opener") {
		InnerLogTestLoop<LOG>(data,bind_data, tuples_to_create, output, *context.client_data->file_opener);
	} else {
		throw InvalidInputException("Invalid logger type: %s", logger);
	}
}

unique_ptr<LocalTableFunctionState> TestLoggingInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
																		   GlobalTableFunctionState *global_state) {
	return make_uniq<TestLoggingData>(context);
}

static
void TestLoggingFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.local_state->Cast<TestLoggingData>();
	auto &bind_data = data_p.bind_data->Cast<TestLoggingBindData>();

	idx_t tuples_to_create = MinValue(bind_data.total_tuples - data.current_tuple, static_cast<idx_t>(STANDARD_VECTOR_SIZE));

	if (bind_data.logging_enabled) {
		LogTest<true>(data,bind_data, tuples_to_create, output, context, bind_data.logger);
	} else {
		LogTest<false>(data, bind_data, tuples_to_create, output, context, bind_data.logger);
	}
}

void TestLoggingFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction logs_fun("test_logging", {LogicalType::INTEGER}, TestLoggingFunction, TestLoggingBind, nullptr, TestLoggingInitLocal);
	logs_fun.named_parameters["enable_logging"] = LogicalType::BOOLEAN;
	logs_fun.named_parameters["log_message"] = LogicalType::VARCHAR;
	logs_fun.named_parameters["logger"] = LogicalType::VARCHAR;
	set.AddFunction(logs_fun);
}

} // namespace duckdb
