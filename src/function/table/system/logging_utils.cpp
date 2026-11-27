#include "duckdb/function/table/system_functions.hpp"

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

	//! The type of the default log sink, or empty to keep its type
	string sink_type;
	case_insensitive_map_t<Value> sink_config;
	LogSinkFilterUpdate filter_update;
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

//! Sets the level of a sink
static void SetLevel(const string &function_name, const Value &value, LogSinkFilterUpdate &filter_update) {
	if (value.IsNull()) {
		throw InvalidInputException("%s: level cannot be NULL", function_name);
	}
	filter_update.set_level = true;
	filter_update.level = EnumUtil::FromString<LogLevel>(value.ToString());
}

//! Adds a log type, or a list of them, to the log types of a sink
static void AddLogTypes(const string &function_name, const Value &value, LogSinkFilterUpdate &filter_update) {
	if (value.IsNull()) {
		throw InvalidInputException("%s: the log types cannot be NULL", function_name);
	}
	filter_update.set_log_types = true;
	if (value.type() == LogicalType::VARCHAR) {
		filter_update.log_types.push_back(value.GetValue<string>());
	} else if (value.type().id() == LogicalTypeId::LIST &&
	           (ListValue::GetChildren(value).empty() ||
	            ListType::GetChildType(value.type()) == LogicalType::VARCHAR)) {
		// an empty list, of any type, is every log type
		for (const auto &child : ListValue::GetChildren(value)) {
			filter_update.log_types.push_back(child.GetValue<string>());
		}
	} else {
		throw BinderException("%s: the log types must be a VARCHAR or a VARCHAR[]", function_name);
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

//! Verifies the log types before changing any of the logging settings. A type of sink is easily taken for a log type,
//! the hint tells how to give it instead
static void VerifyLogTypes(ClientContext &context, const vector<string> &log_types, const string &sink_type_hint) {
	auto &log_manager = context.db->GetLogManager();
	for (const auto &log_type : log_types) {
		TryAutoloadLogTypeExtension(context, log_type);
		if (log_manager.LookupLogType(log_type)) {
			continue;
		}
		auto log_type_to_lower = StringUtil::Lower(log_type);
		if (LogConfig::IsBuiltInLogSink(log_type_to_lower) || log_manager.GetRegisteredLogSink(log_type_to_lower)) {
			throw InvalidInputException("Unknown log type: '%s'. '%s' is a type of log sink, did you mean %s?",
			                            log_type, log_type, StringUtil::Format(sink_type_hint, log_type_to_lower));
		}
		throw InvalidInputException("Unknown log type: '%s'", log_type);
	}
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

//! The sink the CLI registers to print the log entries to the console, and its deprecated name
static constexpr const char *SHELL_LOG_SINK = "shell_log_sink";
static constexpr const char *DEPRECATED_SHELL_LOG_SINK = "shell_log_storage";

static bool IsShellLogSink(const string &sink_type) {
	return StringUtil::CIEquals(sink_type, SHELL_LOG_SINK) ||
	       StringUtil::CIEquals(sink_type, DEPRECATED_SHELL_LOG_SINK);
}

//! In the CLI the default log sink prints the warnings to the console, so changing it can stop printing them
static void WarnIfWarningsAreNoLongerPrinted(ClientContext &context, const EnableLoggingBindData &bind_data) {
	auto default_log_sink = context.db->GetLogManager().GetConfigAndSinks().default_log_sink;
	if (!IsShellLogSink(default_log_sink.type)) {
		return;
	}
	auto &filter_update = bind_data.filter_update;
	const bool leaves_shell_sink = !bind_data.sink_type.empty() && !IsShellLogSink(bind_data.sink_type);
	const bool drops_warnings = (filter_update.set_level && filter_update.level > LogLevel::LOG_WARNING) ||
	                            (filter_update.set_log_types && !filter_update.log_types.empty());
	if (!leaves_shell_sink && !drops_warnings) {
		return;
	}
	string hint;
	if (leaves_shell_sink) {
		hint = "To keep printing warnings to the console, also run CALL enable_log_sink('shell_log_sink', level := "
		       "'warning').";
	} else {
		// the same filter on a sink of its own leaves the console alone
		string parameters;
		if (filter_update.set_level) {
			parameters +=
			    StringUtil::Format(", level := '%s'", StringUtil::Lower(EnumUtil::ToString(filter_update.level)));
		}
		if (filter_update.set_log_types) {
			vector<string> log_types;
			for (auto &log_type : filter_update.log_types) {
				log_types.push_back("'" + log_type + "'");
			}
			parameters += StringUtil::Format(", log_types := [%s]", StringUtil::Join(log_types, ", "));
		}
		hint = StringUtil::Format("The default log sink prints the warnings to the console. To log other entries "
		                          "instead, enable a log sink for them, e.g. CALL enable_log_sink('my_logs', type := "
		                          "'memory'%s).",
		                          parameters);
	}
	DUCKDB_LOG_WARNING(context,
	                   "The logging settings have been changed so you may lose warnings printed in the CLI.\n%s\n"
	                   "For more info see https://duckdb.org/docs/current/operations_manual/logging/overview.",
	                   hint)
}

static void EnableLogging(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto bind_data = data.bind_data->Cast<EnableLoggingBindData>();

	for (auto &parameter : bind_data.deprecated_parameters) {
		DUCKDB_LOG_WARNING(*context.db, "the %s parameter of enable_logging is deprecated: use %s instead", parameter,
		                   GetSinkParameterName(parameter));
	}

	auto &log_manager = context.db->GetLogManager();
	WarnIfWarningsAreNoLongerPrinted(context, bind_data);
	log_manager.WarnIfPrintedTwice(*context.db, LogConfig::DEFAULT_LOG_SINK_NAME, bind_data.sink_type,
	                               bind_data.filter_update);

	// a prepared call can run after the sink it was bound against changed
	VerifyFileSinkPath(log_manager, bind_data.sink_type, bind_data.sink_config);

	// enable_logging configures the default log sink, a failed change leaves the sinks as they were
	log_manager.EnableLogSink(*context.db, LogConfig::DEFAULT_LOG_SINK_NAME, bind_data.sink_type, bind_data.sink_config,
	                          bind_data.filter_update);
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
	config.CheckLock(LoggingStorage::Name);

	auto result = make_uniq<EnableLoggingBindData>();

	bool sink_isset = false;
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
			AddLogTypes("EnableLogging", param.second, result->filter_update);
		} else if (key == "level") {
			SetLevel("EnableLogging", param.second, result->filter_update);
		} else if (key == "sink") {
			sink_isset = true;
			result->sink_type = param.second.ToString();
		} else if (!SetSinkParameter("EnableLogging", key, name, param.second, result->sink_config)) {
			throw InvalidInputException("EnableLogging: unknown named parameter: %s", param.first.c_str());
		}
	}

	// A 'path' in sink_config (provided either via sink_path= or sink_config={path:...}) implies the file sink.
	// Without a sink or a path, the default log sink keeps its type
	if (!sink_isset && result->sink_config.find("path") != result->sink_config.end()) {
		result->sink_type = LogConfig::FILE_STORAGE_NAME;
	}

	VerifyFileSinkPath(context.db->GetLogManager(), result->sink_type, result->sink_config);

	// the log types by position, as before log_types existed
	if (!input.inputs.empty()) {
		if (result->filter_update.set_log_types) {
			throw InvalidInputException(
			    "EnableLogging: give the log types either by position or as log_types, not both");
		}
		AddLogTypes("EnableLogging", input.inputs[0], result->filter_update);
	}
	VerifyLogTypes(context, result->filter_update.log_types, "CALL enable_logging(type := '%s')");

	// as in 2.0.0, enable_logging sets the whole filter of the default log sink: the level and the log types that are
	// not given go back to the default level with every log type, and the log types are logged at their own level
	result->filter_update.reset = true;
	if (!result->filter_update.log_types.empty()) {
		result->filter_update.set_level = false;
	}

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return std::move(result);
}

//! Turns the default log sink off
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
	//! The type of the sink to enable, or empty to keep the type of an existing sink, or to name it after its type
	string type;
	case_insensitive_map_t<Value> sink_config;
	LogSinkFilterUpdate filter_update;
};

//! The configuration lock locks the log sinks as well, unless these logging settings are allowed
static void CheckLogSinkLock(ClientContext &context, const char *function_name, const vector<Identifier> &settings) {
	auto &config = DBConfig::GetConfig(context);
	try {
		for (auto &setting : settings) {
			config.CheckLock(setting);
		}
	} catch (InvalidInputException &) {
		throw InvalidInputException("Cannot call %s - the configuration has been locked", function_name);
	}
}

static unique_ptr<FunctionData> BindLogSinkName(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
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

static unique_ptr<FunctionData> BindDisableLogSink(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	// turning a sink off is turning logging off for it
	CheckLogSinkLock(context, "disable_log_sink", {EnableLogging::Name});
	return BindLogSinkName(context, input, return_types, names);
}

static unique_ptr<FunctionData> BindEnableLogSink(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	CheckLogSinkLock(context, "enable_log_sink",
	                 {EnableLogging::Name, EnabledLogTypes::Name, LoggingLevel::Name, LoggingStorage::Name});
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
		} else if (name == "level") {
			SetLevel("enable_log_sink", param.second, bind_data.filter_update);
		} else if (name == "log_types") {
			AddLogTypes("enable_log_sink", param.second, bind_data.filter_update);
		} else {
			throw InvalidInputException("enable_log_sink: unknown named parameter: %s", name);
		}
	}
	VerifyLogTypes(context, bind_data.filter_update.log_types, "type := '%s'");
	return result;
}

//! Sends the log entries to this sink
static void EnableLogSink(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<LogSinkNameBindData>();
	auto &log_manager = context.db->GetLogManager();
	log_manager.WarnIfPrintedTwice(*context.db, bind_data.name, bind_data.type, bind_data.filter_update);
	log_manager.EnableLogSink(*context.db, bind_data.name, bind_data.type, bind_data.sink_config,
	                          bind_data.filter_update);
}

//! Stops sending the log entries to this sink, which keeps its log entries
static void DisableLogSink(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<LogSinkNameBindData>();
	context.db->GetLogManager().DisableLogSink(*context.db, bind_data.name);
}

void EnableLoggingFun::RegisterFunction(BuiltinFunctions &set) {
	auto enable_fun = TableFunction("enable_logging", {}, EnableLogging, BindEnableLogging, nullptr, nullptr);

	enable_fun.GetSignature().AddArgs("args", LogicalType::ANY).WithTypedKwargs("options", [](TypedKwargs &options) {
		AddSinkParameters(options.Add("level", LogicalType::VARCHAR)
		                      .Add("log_types", LogicalType::ANY)
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
		options.Add("type", LogicalType::VARCHAR)
		    .Add("level", LogicalType::VARCHAR)
		    .Add("log_types", LogicalType::ANY)
		    .Add("config", LogicalType::ANY);
	});
	// enables a log sink per execution
	enable_sink_fun.stability = FunctionStability::VOLATILE;
	set.AddFunction(enable_sink_fun);

	auto disable_sink_fun =
	    TableFunction("disable_log_sink", {LogicalType::VARCHAR}, DisableLogSink, BindDisableLogSink, nullptr, nullptr);
	disable_sink_fun.stability = FunctionStability::VOLATILE;
	set.AddFunction(disable_sink_fun);
}

} // namespace duckdb
