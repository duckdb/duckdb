#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

static void EnableProfiling(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	context.EnableProfiling();
}

static unique_ptr<FunctionData> BindEnableProfiling(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto config = ClientConfig::GetConfig(context);

	string format = "";
	bool format_set = false;
	string save_location = "";
	bool save_location_set = false;

	for (const auto &named_param : input.named_parameters) {
		auto key = StringUtil::Lower(named_param.first);
		if (key == "format") {
			format = StringUtil::Lower(named_param.second.ToString());
			format_set = true;
		} else if (key == "coverage") {
			ProfilingCoverageSetting::SetLocal(context, named_param.second);
		} else if (key == "save_location") {
			save_location = StringUtil::Lower(named_param.second.ToString());
			save_location_set = true;
		} else if (key == "mode") {
			ProfilingModeSetting::SetLocal(context, named_param.second);
		} else if (key == "enabled_metrics") {
			if (named_param.second.type().id() != LogicalTypeId::STRUCT) {
				throw InvalidInputException("EnableProfiling: enabled_metrics must be a struct");
			}
			CustomProfilingSettingsSetting::SetLocal(context, named_param.second);
		} else {
			throw InvalidInputException("EnableProfiling: unknown parameter: %s", key.c_str());
		}
	}

	if (format_set && save_location_set) {
		auto &file_system = FileSystem::GetFileSystem(context);
		const auto file_type = file_system.ExtractExtension(save_location);
		if (file_type != "txt" && file_type != format) {
			throw InvalidInputException(
			    "EnableProfiling: the save_location must be a .txt file or match the specified format.");
		}
	}

	if (format_set) {
		EnableProfilingSetting::SetLocal(context, input.named_parameters.at("format"));
	}

	if (save_location_set) {
		ProfileOutputSetting::SetLocal(context, input.named_parameters.at("save_location"));
	}

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return nullptr;
}

static void DisableProfiling(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	context.DisableProfiling();
}

static unique_ptr<FunctionData> BindDisableProfiling(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return nullptr;
}

void EnableProfilingFun::RegisterFunction(BuiltinFunctions &set) {
	auto enable_fun = TableFunction("enable_profiling", {}, EnableProfiling, BindEnableProfiling, nullptr, nullptr);

	enable_fun.named_parameters.emplace("format", LogicalType::VARCHAR);
	enable_fun.named_parameters.emplace("coverage", LogicalType::VARCHAR);
	enable_fun.named_parameters.emplace("save_location", LogicalType::VARCHAR);
	enable_fun.named_parameters.emplace("mode", LogicalType::VARCHAR);
	enable_fun.named_parameters.emplace("enabled_metrics", LogicalType::ANY);

	enable_fun.varargs = LogicalType::ANY;
	set.AddFunction(enable_fun);

	auto disable_fun = TableFunction("disable_profiling", {}, DisableProfiling, BindDisableProfiling, nullptr, nullptr);
	set.AddFunction(disable_fun);
}

} // namespace duckdb
