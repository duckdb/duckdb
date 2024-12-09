#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

struct TestLoggingBindData : public TableFunctionData {
	idx_t log_entries_per_tuple = 1;
	idx_t total_tuples = 10;
	bool logging_enabled = true;
	string log_message = "test_logging test message";
	string logger = "client_context";
};

struct TestLoggingData : public GlobalTableFunctionState {
	idx_t current_tuple = 0;
};

static unique_ptr<FunctionData> TestLoggingBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto res = make_uniq<TestLoggingBindData>();

	res->total_tuples = input.inputs[0].GetValue<idx_t>();

	for (const auto &param : input.named_parameters) {
		auto param_name = StringUtil::Lower(param.first);
		if (param_name == "enable_logging") {
			res->logging_enabled = param.second.GetValue<bool>();
		} else if (param_name == "log_entries_per_tuple") {
			res->log_entries_per_tuple = param.second.GetValue<idx_t>();
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

template <bool LOG, bool REQUIRE_LOOP=false, bool USE_GLOBAL_LOGGER=false>
static void InnerLogTestLoop(TestLoggingData& data, const TestLoggingBindData &bind_data, idx_t tuples_to_create, DataChunk &output, ClientContext &context) {
	for (idx_t i = 0; i < tuples_to_create; i++) {
		if(LOG) {
			if (REQUIRE_LOOP) {
				for (idx_t j = 0; j < bind_data.log_entries_per_tuple; j++) {
					if (USE_GLOBAL_LOGGER) {
						Logger::Info(*context.db, bind_data.log_message.c_str());
					} else {
						Logger::Info(context, bind_data.log_message.c_str());
					}
				}
			} else {
				if (USE_GLOBAL_LOGGER) {
					Logger::Info(*context.db, bind_data.log_message.c_str());
				} else {
					Logger::Info(context, bind_data.log_message.c_str());
				}

			}
		}
		FlatVector::GetData<idx_t>(output.data[0])[i] = data.current_tuple++;
	}
	output.SetCardinality(tuples_to_create);
}

template <bool LOG, bool REQUIRE_LOOP=false>
static void InnerLogTestLoop(TestLoggingData& data, const TestLoggingBindData &bind_data, idx_t tuples_to_create, DataChunk &output, ClientContext &context, bool use_global_logger) {
	if (use_global_logger) {
		InnerLogTestLoop<LOG,REQUIRE_LOOP, true>(data,bind_data, tuples_to_create, output, context);
	} else {
		InnerLogTestLoop<LOG,REQUIRE_LOOP>(data,bind_data, tuples_to_create, output, context);
	}
}

unique_ptr<GlobalTableFunctionState> TestLoggingInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DuckDBLogData>();
}

static
void TestLoggingFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<TestLoggingData>();
	auto &bind_data = data_p.bind_data->Cast<TestLoggingBindData>();

	idx_t tuples_to_create = MinValue(bind_data.total_tuples - data.current_tuple, static_cast<idx_t>(STANDARD_VECTOR_SIZE));

	bool use_global_logger = bind_data.logger == "global";

	if (bind_data.logging_enabled) {
		if (bind_data.log_entries_per_tuple > 1) {
			InnerLogTestLoop<true, true>(data,bind_data, tuples_to_create, output, context, use_global_logger);
		} else {
			InnerLogTestLoop<true>(data,bind_data, tuples_to_create, output, context, use_global_logger);
		}
	} else {
		InnerLogTestLoop<false>(data, bind_data, tuples_to_create, output, context, use_global_logger);
	}
}

void TestLoggingFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction logs_fun("test_logging", {LogicalType::INTEGER}, TestLoggingFunction, TestLoggingBind, TestLoggingInit);
	logs_fun.named_parameters["log_entries_per_tuple"] = LogicalType::UBIGINT;
	logs_fun.named_parameters["enable_logging"] = LogicalType::BOOLEAN;
	logs_fun.named_parameters["log_message"] = LogicalType::VARCHAR;
	logs_fun.named_parameters["logger"] = LogicalType::VARCHAR;
	set.AddFunction(logs_fun);
}

} // namespace duckdb
