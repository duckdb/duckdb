#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog_entry/collate_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class EnableLoggingBindData : public TableFunctionData {
public:
	EnableLoggingBindData(case_insensitive_map_t<Value> config, vector<Value> inputs_p)
	    : storage_config(std::move(config)), inputs(std::move(inputs_p)) {
	}
	EnableLoggingBindData() {
	}

	case_insensitive_map_t<Value> storage_config;
	vector<Value> inputs;
};

static void EnableLogging(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto bind_data = data.bind_data->Cast<EnableLoggingBindData>();

	if (!bind_data.storage_config.empty()) {
		context.db->GetLogManager().UpdateLogStorageConfig(*context.db, bind_data.storage_config);
	}

	if (bind_data.inputs.empty()) {
		context.db->GetLogManager().SetEnableLogging(true);
		context.db->GetLogManager().SetLogMode(LogMode::LEVEL_ONLY);
		return;
	}

	vector<string> types;
	if (bind_data.inputs[0].type() == LogicalType::VARCHAR) {
		types.push_back(bind_data.inputs[0].GetValue<string>());
	} else if (bind_data.inputs[0].type() == LogicalType::LIST(LogicalType::VARCHAR)) {
		for (const auto &child : ListValue::GetChildren(bind_data.inputs[0])) {
			types.push_back(child.GetValue<string>());
		}
	} else {
		throw InvalidInputException("Unexpected type for PragmaEnableLogging");
	}

	context.db->GetLogManager().SetEnableStructuredLoggers(types);
}

static unique_ptr<FunctionData> BindEnableLogging(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() > 1) {
		throw InvalidInputException("PragmaEnableLogging: expected 0 or 1 parameter");
	}

	case_insensitive_map_t<Value> storage_config;

	for (const auto &param : input.named_parameters) {
		if (StringUtil::Lower(param.first) == "level") {
			context.db->GetLogManager().SetLogLevel(EnumUtil::FromString<LogLevel>(param.second.ToString()));
		} else if (StringUtil::Lower(param.first) == "storage") {
			context.db->GetLogManager().SetLogStorage(*context.db, param.second.ToString());
		} else if (StringUtil::Lower(param.first) == "storage_config") {
			auto &children = StructValue::GetChildren(param.second);
			for (idx_t i = 0; i < children.size(); i++) {
				storage_config[StructType::GetChildName(param.second.type(), i)] = children[i];
			}
		} else {
			throw InvalidInputException("PragmaEnableLogging: unknown named parameter: %s", param.first.c_str());
		}
	}

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return make_uniq<EnableLoggingBindData>(std::move(storage_config), input.inputs);
}

static void DisableLogging(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	context.db->GetLogManager().SetEnableLogging(false);
	// TODO: clean this sht up
	context.db->GetLogManager().SetLogMode(LogMode::LEVEL_ONLY);
	context.db->GetLogManager().SetLogLevel(LogConfig::DEFAULT_LOG_LEVEL);
}

static unique_ptr<FunctionData> BindDisableLogging(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return make_uniq<EnableLoggingBindData>();
}

void EnableLoggingFun::RegisterFunction(BuiltinFunctions &set) {
	auto enable_fun = TableFunction("enable_logging", {}, EnableLogging, BindEnableLogging, nullptr, nullptr);

	enable_fun.named_parameters.emplace("level", LogicalType::VARCHAR);
	enable_fun.named_parameters.emplace("storage", LogicalType::VARCHAR);
	enable_fun.named_parameters.emplace("storage_config", LogicalType::ANY);

	enable_fun.varargs = LogicalType::ANY;
	set.AddFunction(enable_fun);

	auto disable_fun = TableFunction("disable_logging", {}, DisableLogging, BindDisableLogging, nullptr, nullptr);
	set.AddFunction(disable_fun);
}

} // namespace duckdb
