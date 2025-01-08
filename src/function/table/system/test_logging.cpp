#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct TestLoggingBindData : public TableFunctionData {
	idx_t total_tuples = 10;
	bool logging_enabled = true;
	string log_message = "test_logging test message";
	string logger = "client_context";
	LogLevel level = LogLevel::L_INFO;
	string log_type = "";
};

struct TestLoggingData : public LocalTableFunctionState {
	explicit TestLoggingData(ExecutionContext &context_p) : context(context_p), current_tuple(0) {};
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
		} else if (param_name == "level") {
			res->level = EnumUtil::FromString<LogLevel>(param.second.GetValue<string>());
		} else if (param_name == "log_type") {
			res->log_type = param.second.GetValue<string>();
		} else {
			throw InvalidInputException("Unknown input passed: '%s'", param_name);
		}
	}

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::UBIGINT);

	return std::move(res);
}

template <bool LOG, class LOGGER, bool WITH_TYPE>
static void InnerLogTestLoop(TestLoggingData &data, const TestLoggingBindData &bind_data, idx_t tuples_to_create,
                             DataChunk &output, LOGGER &source) {
	for (idx_t i = 0; i < tuples_to_create; i++) {
		if (LOG) {
			if (WITH_TYPE) {
				Logger::Log(bind_data.log_type.c_str(), source, bind_data.level, bind_data.log_message.c_str());
			} else {
				Logger::Log(source, bind_data.level, bind_data.log_message.c_str());
			}
		}
		FlatVector::GetData<idx_t>(output.data[0])[i] = data.current_tuple++;
	}
	output.SetCardinality(tuples_to_create);
}

template <bool LOG, bool WITH_LOG_TYPE>
static void LogTest(TestLoggingData &data, const TestLoggingBindData &bind_data, idx_t tuples_to_create,
                    DataChunk &output, ClientContext &context, const string &logger) {
	if (logger == "global") {
		InnerLogTestLoop<LOG, DatabaseInstance, WITH_LOG_TYPE>(data, bind_data, tuples_to_create, output, *context.db);
	} else if (logger == "client_context") {
		InnerLogTestLoop<LOG, ClientContext, WITH_LOG_TYPE>(data, bind_data, tuples_to_create, output, context);
	} else if (logger == "thread_local") {
		InnerLogTestLoop<LOG, ExecutionContext, WITH_LOG_TYPE>(data, bind_data, tuples_to_create, output, data.context);
	} else if (logger == "file_opener") {
		InnerLogTestLoop<LOG, FileOpener, WITH_LOG_TYPE>(data, bind_data, tuples_to_create, output,
		                                                 *context.client_data->file_opener);
	} else {
		throw InvalidInputException("Invalid logger type: %s", logger);
	}
}

unique_ptr<LocalTableFunctionState> TestLoggingInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                         GlobalTableFunctionState *global_state) {
	return make_uniq<TestLoggingData>(context);
}

static void TestLoggingFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.local_state->Cast<TestLoggingData>();
	auto &bind_data = data_p.bind_data->Cast<TestLoggingBindData>();

	idx_t tuples_to_create =
	    MinValue(bind_data.total_tuples - data.current_tuple, static_cast<idx_t>(STANDARD_VECTOR_SIZE));

	if (bind_data.logging_enabled) {
		if (!bind_data.log_type.empty()) {
			LogTest<true, true>(data, bind_data, tuples_to_create, output, context, bind_data.logger);
		} else {
			LogTest<true, false>(data, bind_data, tuples_to_create, output, context, bind_data.logger);
		}
	} else {
		LogTest<false, false>(data, bind_data, tuples_to_create, output, context, bind_data.logger);
	}
}

void TestLoggingFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction logs_fun("test_logging", {LogicalType::INTEGER}, TestLoggingFunction, TestLoggingBind, nullptr,
	                       TestLoggingInitLocal);
	logs_fun.named_parameters["enable_logging"] = LogicalType::BOOLEAN;
	logs_fun.named_parameters["log_message"] = LogicalType::VARCHAR;
	logs_fun.named_parameters["logger"] = LogicalType::VARCHAR;
	logs_fun.named_parameters["level"] = LogicalType::VARCHAR;
	logs_fun.named_parameters["log_type"] = LogicalType::VARCHAR;
	set.AddFunction(logs_fun);
}

} // namespace duckdb
