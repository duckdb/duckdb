#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog_entry/collate_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/logging/log_sink.hpp"
#include "duckdb/logging/logging.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_entries.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

class EnableLoggingBindData : public TableFunctionData {
public:
	EnableLoggingBindData() {
	}

	case_insensitive_map_t<Value> sink_config;
	LogConfig config;
	vector<string> log_types_to_set;
	//! The deprecated parameters that were used, to warn about them
	vector<string> deprecated_parameters;
};

//! The sink parameter that replaces a deprecated storage parameter, e.g. storage_path -> sink_path
static string GetSinkParameterName(const string &storage_parameter) {
	return "sink" + storage_parameter.substr(strlen("storage"));
}

//! The sink parameters of enable_logging
static TypedKwargs &AddSinkParameters(TypedKwargs &options) {
	// sink_path, sink_normalize and sink_buffer_size are forwarded to the sink_config struct as syntactic sugar
	return options.Add("sink_config", LogicalType::ANY)
	    .Add("sink_path", LogicalType::VARCHAR)
	    .Add("sink_normalize", LogicalType::BOOLEAN)
	    .Add("sink_buffer_size", LogicalType::UBIGINT);
}

//! Puts the fields of a config struct into the sink config
static void SetSinkConfig(const string &function_name, const string &name, const Value &value,
                          case_insensitive_map_t<Value> &sink_config) {
	if (value.IsNull() || value.type().id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("%s: %s must be a struct", function_name, name);
	}
	auto &children = StructValue::GetChildren(value);
	for (idx_t i = 0; i < children.size(); i++) {
		sink_config[StructType::GetChildName(value.type(), i).GetIdentifierName()] = children[i];
	}
}

//! Puts a sink parameter into the sink config, returns false if the key is not a sink parameter
static bool SetSinkParameter(const string &function_name, const string &key, const string &name, const Value &value,
                             case_insensitive_map_t<Value> &sink_config) {
	if (key == "sink_config") {
		SetSinkConfig(function_name, name, value, sink_config);
		return true;
	}
	if (key == "sink_path" || key == "sink_normalize" || key == "sink_buffer_size") {
		sink_config[key.substr(strlen("sink_"))] = value;
		return true;
	}
	return false;
}

//! File logging requires a path. Reject switching to the file sink without one before mutating any state, so the
//! active sink is preserved instead of becoming a path-less sink that throws on every later flush (end-of-query
//! and shutdown included).
static void VerifyFileSinkPath(LogManager &log_manager, const string &sink,
                               const case_insensitive_map_t<Value> &sink_config) {
	if (StringUtil::Lower(sink) != LogConfig::FILE_STORAGE_NAME) {
		return;
	}
	// A default file sink keeps its existing path, only a fresh switch is guarded.
	if (log_manager.GetConfig().storage == LogConfig::FILE_STORAGE_NAME) {
		return;
	}
	auto path_entry = sink_config.find("path");
	bool has_usable_path =
	    path_entry != sink_config.end() && !path_entry->second.IsNull() && !path_entry->second.ToString().empty();
	if (!has_usable_path) {
		throw InvalidInputException("Cannot enable the 'file' log sink without a valid path. Provide one in config, "
		                            "e.g. CALL enable_logging(type := 'file', config := {path: 'mylog.csv'});");
	}
}

//! Warnings stop being printed in the CLI when they no longer reach the shell's sink
static constexpr const char *SHELL_LOG_SINK = "shell_log_sink";
static constexpr const char *DEPRECATED_SHELL_LOG_SINK = "shell_log_storage";

//! Whether the shell's sink receives the log entries, as the default log sink or under a name
static bool IsShellLogSinkEnabled(LogManager &log_manager) {
	auto sinks = log_manager.GetConfigAndSinks();
	if (StringUtil::CIEquals(sinks.default_log_sink.type, SHELL_LOG_SINK)) {
		return true;
	}
	for (auto &entry : sinks.named_log_sinks) {
		if (StringUtil::CIEquals(entry.second.type, SHELL_LOG_SINK)) {
			return true;
		}
	}
	return false;
}

static void EnableLogging(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto bind_data = data.bind_data->Cast<EnableLoggingBindData>();

	for (auto &parameter : bind_data.deprecated_parameters) {
		DUCKDB_LOG_WARNING(*context.db, "the %s parameter of enable_logging is deprecated: use %s instead", parameter,
		                   GetSinkParameterName(parameter));
	}

	auto &log_manager = context.db->GetLogManager();

	// a shell sink enabled under a name keeps receiving the log entries when the default log sink changes
	const bool leaves_shell_sink = StringUtil::CIEquals(log_manager.GetConfig().storage, SHELL_LOG_SINK) &&
	                               !StringUtil::CIEquals(bind_data.config.storage, SHELL_LOG_SINK) &&
	                               !StringUtil::CIEquals(bind_data.config.storage, DEPRECATED_SHELL_LOG_SINK);
	const bool drops_warnings = bind_data.config.level > LogLevel::LOG_WARNING || !bind_data.log_types_to_set.empty();
	if (IsShellLogSinkEnabled(log_manager) && (leaves_shell_sink || drops_warnings)) {
		// enabling the shell's sink by name only helps when leaving it, the level and the log types apply to every sink
		auto hint = drops_warnings ? "The log level and the log types apply to every log sink, including the one that "
		                             "prints warnings to the console."
		                           : "To keep printing warnings to the console, also run "
		                             "CALL enable_log_sink('shell_log_sink').";
		DUCKDB_LOG_WARNING(context,
		                   "The logging settings have been changed so you may lose warnings printed in the CLI.\n%s\n"
		                   "For more info see https://duckdb.org/docs/current/operations_manual/logging/overview.",
		                   hint)
	}

	if (!StringUtil::CIEquals(log_manager.GetConfig().storage, bind_data.config.storage)) {
		log_manager.WarnIfPrintedTwice(*context.db, bind_data.config.storage);
	}

	// a prepared call can run after the sink it was bound against changed
	VerifyFileSinkPath(log_manager, bind_data.config.storage, bind_data.sink_config);

	// Apply the config generated from the input - restoring the previous config if that fails part-way
	auto previous = log_manager.GetConfigAndSinks();
	try {
		log_manager.SetConfig(*context.db, bind_data.config);

		if (bind_data.log_types_to_set.empty()) {
			log_manager.SetEnableLogging(true);
			log_manager.SetLogMode(LogMode::LEVEL_ONLY);
		} else {
			log_manager.SetEnableStructuredLoggers(bind_data.log_types_to_set);
		}

		if (!bind_data.sink_config.empty()) {
			log_manager.UpdateDefaultLogSinkConfig(*context.db, bind_data.sink_config);
		}
	} catch (...) {
		// the previous sinks themselves, as new ones of their kind would lack their sink config
		log_manager.RestoreConfig(std::move(previous));
		throw;
	}
}

//! Log types are registered by extensions when they are loaded, so an unknown type may just belong to an extension
//! that is not loaded yet
static void TryAutoloadLogTypeExtension(ClientContext &context, const string &log_type) {
	auto &db = *context.db;
	if (db.GetLogManager().LookupLogType(log_type)) {
		return;
	}
	ExtensionHelper::TryAutoloadFromEntry(db, Identifier(log_type), EXTENSION_LOG_TYPES);
}

//! Adds a log type, or a list of them, to log_types
static void AddLogTypes(const Value &value, vector<string> &log_types) {
	if (value.IsNull()) {
		throw InvalidInputException("EnableLogging: the log types cannot be NULL");
	}
	if (value.type() == LogicalType::VARCHAR) {
		log_types.push_back(value.GetValue<string>());
	} else if (value.type() == LogicalType::LIST(LogicalType::VARCHAR)) {
		for (const auto &child : ListValue::GetChildren(value)) {
			log_types.push_back(child.GetValue<string>());
		}
	} else {
		throw BinderException("EnableLogging: the log types must be a VARCHAR or a VARCHAR[]");
	}
}

static unique_ptr<FunctionData> BindEnableLogging(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs.size() > 1) {
		throw InvalidInputException("EnableLogging: expected 0 or 1 parameter");
	}

	auto &config = DBConfig::GetConfig(context);
	config.CheckLock(EnableLogging::Name);
	config.CheckLock(EnabledLogTypes::Name);
	config.CheckLock(LoggingLevel::Name);
	config.CheckLock(LoggingMode::Name);
	config.CheckLock(LoggingSink::Name);

	auto result = make_uniq<EnableLoggingBindData>();

	bool sink_isset = false;
	bool types_isset = false;
	//! The name each parameter was given by, under the name of the parameter it is: storage_path is sink_path
	case_insensitive_map_t<string> given_parameters;

	for (const auto &param : input.named_parameters) {
		auto &name = param.first.GetIdentifierName();
		auto key = name;
		if (StringUtil::StartsWith(key, "storage")) {
			result->deprecated_parameters.push_back(name);
			key = GetSinkParameterName(key);
		} else if (key == "config") {
			key = "sink_config";
		} else if (key == "type") {
			// the kind of the default log sink, the same name as in enable_log_sink
			key = "sink";
		}
		auto given = given_parameters.emplace(key, name);
		if (!given.second) {
			auto &previous = given.first->second;
			if (StringUtil::StartsWith(name, "storage") || StringUtil::StartsWith(previous, "storage")) {
				auto &deprecated = StringUtil::StartsWith(name, "storage") ? name : previous;
				auto &current = StringUtil::StartsWith(name, "storage") ? previous : name;
				throw InvalidInputException("EnableLogging: %s cannot be combined with its deprecated name %s", current,
				                            deprecated);
			}
			throw InvalidInputException("EnableLogging: %s cannot be combined with %s, they are the same parameter",
			                            previous, name);
		}
		if (key == "log_types") {
			types_isset = true;
			AddLogTypes(param.second, result->log_types_to_set);
		} else if (key == "level") {
			if (param.second.IsNull()) {
				throw InvalidInputException("EnableLogging: level cannot be NULL");
			}
			result->config.level = EnumUtil::FromString<LogLevel>(param.second.ToString());
		} else if (key == "sink") {
			sink_isset = true;
			result->config.storage = param.second.ToString();
		} else if (!SetSinkParameter("EnableLogging", key, name, param.second, result->sink_config)) {
			throw InvalidInputException("EnableLogging: unknown named parameter: %s", param.first.c_str());
		}
	}

	// If the user didn't explicitly set sink=, infer it. A 'path' in sink_config (provided
	// either via sink_path= or sink_config={path:...}) implies the file sink. Otherwise
	// preserve the currently configured sink so logging_sink isn't silently reset.
	if (!sink_isset) {
		if (result->sink_config.find("path") != result->sink_config.end()) {
			result->config.storage = LogConfig::FILE_STORAGE_NAME;
		} else {
			result->config.storage = context.db->GetLogManager().GetConfig().storage;
		}
	}

	VerifyFileSinkPath(context.db->GetLogManager(), result->config.storage, result->sink_config);

	// the log types by position, as before types existed
	if (!input.inputs.empty()) {
		if (types_isset) {
			throw InvalidInputException(
			    "EnableLogging: give the log types either by position or as log_types, not both");
		}
		AddLogTypes(input.inputs[0], result->log_types_to_set);
	}

	auto &log_manager = context.db->GetLogManager();
	for (const auto &log_type : result->log_types_to_set) {
		TryAutoloadLogTypeExtension(context, log_type);
		// verify the log types before changing any of the logging settings
		if (log_manager.LookupLogType(log_type)) {
			continue;
		}
		// the first argument is easily taken for the sink
		auto log_type_to_lower = StringUtil::Lower(log_type);
		if (LogConfig::IsBuiltInLogSink(log_type_to_lower) || log_manager.GetRegisteredLogSink(log_type_to_lower)) {
			throw InvalidInputException("Unknown log type: '%s'. '%s' is a type of log sink, did you mean "
			                            "CALL enable_logging(type := '%s')?",
			                            log_type, log_type, log_type_to_lower);
		}
		throw InvalidInputException("Unknown log type: '%s'", log_type);
	}

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return std::move(result);
}

//! Reset the logmanager to the defaults
static void DisableLogging(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	context.db->GetLogManager().SetEnableLogging(false);
}

//! The name of the log sink to truncate, or empty for the default log sink
class TruncateLogsBindData : public TableFunctionData {
public:
	explicit TruncateLogsBindData(string sink_name_p) : sink_name(std::move(sink_name_p)) {
	}

	string sink_name;
};

//! Truncate a log sink, the default log sink unless a name is given
static void TruncateLogs(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<TruncateLogsBindData>();
	context.db->GetLogManager().TruncateLogSink(bind_data.sink_name);
}

static unique_ptr<FunctionData> BindDisableLogging(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	DBConfig::GetConfig(context).CheckLock(EnableLogging::Name);
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return std::move(make_uniq<EnableLoggingBindData>());
}

static unique_ptr<FunctionData> BindTruncateLogs(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return make_uniq<TruncateLogsBindData>(DuckDBLogFun::GetSinkName(input));
}

class LogSinkNameBindData : public TableFunctionData {
public:
	explicit LogSinkNameBindData(string name_p) : name(std::move(name_p)) {
	}

	string name;
	//! The type of the sink to enable, or empty if it is the name
	string type;
	case_insensitive_map_t<Value> sink_config;
};

static unique_ptr<FunctionData> BindLogSinkName(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	DBConfig::GetConfig(context).CheckLock(LoggingSink::Name);
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("The log sink name cannot be NULL");
	}
	auto name = input.inputs[0].GetValue<string>();
	if (name.empty()) {
		throw InvalidInputException("The log sink name cannot be empty");
	}

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return make_uniq<LogSinkNameBindData>(std::move(name));
}

static unique_ptr<FunctionData> BindEnableLogSink(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	// enabling a sink turns logging on
	DBConfig::GetConfig(context).CheckLock(EnableLogging::Name);
	auto result = BindLogSinkName(context, input, return_types, names);
	auto &bind_data = result->Cast<LogSinkNameBindData>();
	for (const auto &param : input.named_parameters) {
		auto &name = param.first.GetIdentifierName();
		if (name == "type") {
			if (param.second.IsNull() || param.second.ToString().empty()) {
				throw InvalidInputException("enable_log_sink: type cannot be NULL or empty");
			}
			bind_data.type = param.second.ToString();
		} else if (name == "config") {
			SetSinkConfig("enable_log_sink", name, param.second, bind_data.sink_config);
		} else {
			throw InvalidInputException("enable_log_sink: unknown named parameter: %s", name);
		}
	}
	return result;
}

//! Also send the log entries to this sink
static void EnableLogSink(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<LogSinkNameBindData>();
	auto &log_manager = context.db->GetLogManager();
	if (!log_manager.IsLogSinkEnabled(bind_data.name)) {
		log_manager.WarnIfPrintedTwice(*context.db, bind_data.type.empty() ? bind_data.name : bind_data.type);
	}
	log_manager.EnableLogSink(*context.db, bind_data.name, bind_data.type, bind_data.sink_config);
}

static void DisableLogSink(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<LogSinkNameBindData>();
	context.db->GetLogManager().DisableLogSink(*context.db, bind_data.name);
}

void EnableLoggingFun::RegisterFunction(BuiltinFunctions &set) {
	auto enable_fun = TableFunction("enable_logging", {}, EnableLogging, BindEnableLogging, nullptr, nullptr);

	enable_fun.GetSignature()
	    .AddArgs("args", LogicalType::ANY)
	    .AddKeywordOnly("level", LogicalType::VARCHAR, Value("INFO"))
	    .WithTypedKwargs("options", [](TypedKwargs &options) {
		    AddSinkParameters(options.Add("log_types", LogicalType::ANY)
		                          .Add("type", LogicalType::VARCHAR)
		                          .Add("sink", LogicalType::VARCHAR)
		                          .Add("config", LogicalType::ANY))
		        // deprecated names of the sink parameters
		        .Add("storage", LogicalType::VARCHAR)
		        .Add("storage_config", LogicalType::ANY)
		        .Add("storage_path", LogicalType::VARCHAR)
		        .Add("storage_normalize", LogicalType::BOOLEAN)
		        .Add("storage_buffer_size", LogicalType::UBIGINT);
	    });

	// reconfigures the log storage per execution
	enable_fun.stability = FunctionStability::VOLATILE;
	set.AddFunction(enable_fun);

	auto disable_fun = TableFunction("disable_logging", {}, DisableLogging, BindDisableLogging, nullptr, nullptr);
	disable_fun.stability = FunctionStability::VOLATILE;
	set.AddFunction(disable_fun);

	auto truncate_fun = TableFunction("truncate_duckdb_logs", {}, TruncateLogs, BindTruncateLogs, nullptr, nullptr);
	DuckDBLogFun::AddSinkParameter(truncate_fun);
	truncate_fun.stability = FunctionStability::VOLATILE;
	set.AddFunction(truncate_fun);

	TableFunction enable_sink_fun("enable_log_sink",
	                              FunctionSignature().AddPositionalOnly("name", LogicalType::VARCHAR), EnableLogSink,
	                              BindEnableLogSink);
	enable_sink_fun.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("type", LogicalType::VARCHAR).Add("config", LogicalType::ANY);
	});
	// enables a log sink per execution
	enable_sink_fun.stability = FunctionStability::VOLATILE;
	set.AddFunction(enable_sink_fun);

	auto disable_sink_fun =
	    TableFunction("disable_log_sink", {LogicalType::VARCHAR}, DisableLogSink, BindLogSinkName, nullptr, nullptr);
	disable_sink_fun.stability = FunctionStability::VOLATILE;
	set.AddFunction(disable_sink_fun);
}

} // namespace duckdb
