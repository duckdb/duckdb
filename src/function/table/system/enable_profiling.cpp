#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

class EnableProfilingBindData : public TableFunctionData {
public:
	EnableProfilingBindData() {
	}

	Value format;
	Value coverage;
	Value save_location;
	Value mode;
	Value metrics;
};

static void EnableProfiling(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto bind_data = data.bind_data->Cast<EnableProfilingBindData>();

	auto &client_config = ClientConfig::GetConfig(context);
	client_config.enable_profiler = true;
	client_config.emit_profiler_output = true;

	if (!bind_data.format.IsNull() && !bind_data.save_location.IsNull()) {
		auto &file_system = FileSystem::GetFileSystem(context);
		const auto file_type = file_system.ExtractExtension(bind_data.save_location.ToString());
		if (file_type != "txt" && file_type != bind_data.format.ToString()) {
			throw InvalidInputException(
			    "EnableProfiling: the save_location must be a .txt file or match the specified format.");
		}

		EnableProfilingSetting::ResetLocal(context);
		ProfileOutputSetting::ResetLocal(context);
	}

	if (!bind_data.format.IsNull()) {
		EnableProfilingSetting::SetLocal(context, bind_data.format);
	}

	if (!bind_data.coverage.IsNull()) {
		ProfilingCoverageSetting::SetLocal(context, bind_data.coverage);
	}

	if (!bind_data.save_location.IsNull()) {
		ProfileOutputSetting::SetLocal(context, bind_data.save_location);
	}

	if (!bind_data.mode.IsNull()) {
		ProfilingModeSetting::SetLocal(context, bind_data.mode);
	}

	if (!bind_data.metrics.IsNull()) {
		CustomProfilingSettingsSetting::SetLocal(context, bind_data.metrics);
	}
}

static unique_ptr<FunctionData> BindEnableProfiling(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() > 1) {
		throw InvalidInputException("EnableProfiling: expected 0 or 1 parameter");
	}

	auto bind_data = make_uniq<EnableProfilingBindData>();

	bool metrics_set = false;

	for (const auto &named_param : input.named_parameters) {
		const auto key = EnumUtil::FromString<ProfilingParameterNames>(named_param.first);
		switch (key) {
		case ProfilingParameterNames::FORMAT:
			bind_data->format = StringUtil::Lower(named_param.second.ToString());
			break;
		case ProfilingParameterNames::COVERAGE:
			bind_data->coverage = StringUtil::Lower(named_param.second.ToString());
			break;
		case ProfilingParameterNames::SAVE_LOCATION:
			bind_data->save_location = named_param.second.ToString();
			break;
		case ProfilingParameterNames::MODE:
			bind_data->mode = StringUtil::Lower(named_param.second.ToString());
			break;
		case ProfilingParameterNames::METRICS: {
			if (named_param.second.type() != LogicalType::LIST(LogicalType::VARCHAR) &&
			    named_param.second.type().id() != LogicalTypeId::STRUCT &&
			    named_param.second.type() != LogicalType::VARCHAR) {
				throw InvalidInputException("EnableProfiling: metrics must be a list of strings or a JSON string");
			}

			bind_data->metrics = named_param.second;
			metrics_set = true;
		}
		}
	}

	// Process positional param: metrics configs
	if (!input.inputs.empty()) {
		if (metrics_set) {
			throw InvalidInputException("EnableProfiling: cannot specify both metrics and positional parameters");
		}
		if (input.inputs[0].type() != LogicalType::LIST(LogicalType::VARCHAR) &&
		    input.inputs[0].type().id() != LogicalTypeId::STRUCT && input.inputs[0].type() != LogicalType::VARCHAR) {
			throw InvalidInputException("EnableProfiling: metrics must be a list of strings or a JSON string");
		}

		bind_data->metrics = input.inputs[0];
	}

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return std::move(bind_data);
}

static void DisableProfiling(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &client_config = ClientConfig::GetConfig(context);
	client_config.enable_profiler = false;
	client_config.emit_profiler_output = false;
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
	enable_fun.named_parameters.emplace("metrics", LogicalType::ANY);

	enable_fun.varargs = LogicalType::LIST(LogicalType::VARCHAR);
	set.AddFunction(enable_fun);

	auto disable_fun = TableFunction("disable_profiling", {}, DisableProfiling, BindDisableProfiling, nullptr, nullptr);
	set.AddFunction(disable_fun);
}

} // namespace duckdb
