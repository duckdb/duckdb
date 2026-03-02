#include "duckdb/main/config.hpp"

#include "duckdb/common/cgroups.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/exception/parser_exception.hpp"

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif

#include <cinttypes>
#include <cstdio>

namespace duckdb {

#ifdef DEBUG
bool DBConfigOptions::debug_print_bindings = false;
#endif

#define DUCKDB_SETTING(_PARAM)                                                                                         \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::Description, _PARAM::InputType, nullptr, nullptr, nullptr, nullptr, nullptr,             \
		    _PARAM::Scope, _PARAM::DefaultValue, nullptr, _PARAM::SettingIndex                                         \
	}
#define DUCKDB_SETTING_CALLBACK(_PARAM)                                                                                \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::Description, _PARAM::InputType, nullptr, nullptr, nullptr, nullptr, nullptr,             \
		    _PARAM::Scope, _PARAM::DefaultValue, _PARAM::OnSet, _PARAM::SettingIndex                                   \
	}
#define DUCKDB_GLOBAL(_PARAM)                                                                                          \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, nullptr, _PARAM::ResetGlobal,         \
		    nullptr, _PARAM::GetSetting, SettingScopeTarget::INVALID, nullptr, nullptr, optional_idx()                 \
	}
#define DUCKDB_LOCAL(_PARAM)                                                                                           \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::Description, _PARAM::InputType, nullptr, _PARAM::SetLocal, nullptr, _PARAM::ResetLocal,  \
		    _PARAM::GetSetting, SettingScopeTarget::INVALID, nullptr, nullptr, optional_idx()                          \
	}
#define DUCKDB_GLOBAL_LOCAL(_PARAM)                                                                                    \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, _PARAM::SetLocal,                     \
		    _PARAM::ResetGlobal, _PARAM::ResetLocal, _PARAM::GetSetting, SettingScopeTarget::INVALID, nullptr,         \
		    nullptr, optional_idx()                                                                                    \
	}
#define FINAL_SETTING                                                                                                  \
	{                                                                                                                  \
		nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, SettingScopeTarget::INVALID, nullptr,  \
		    nullptr, optional_idx()                                                                                    \
	}

#define DUCKDB_SETTING_ALIAS(_ALIAS, _SETTING_INDEX)                                                                   \
	{ _ALIAS, _SETTING_INDEX }
#define FINAL_ALIAS                                                                                                    \
	{ nullptr, 0 }

static const ConfigurationOption internal_options[] = {

    DUCKDB_GLOBAL(AccessModeSetting),
    DUCKDB_SETTING_CALLBACK(AllocatorBackgroundThreadsSetting),
    DUCKDB_GLOBAL(AllocatorBulkDeallocationFlushThresholdSetting),
    DUCKDB_GLOBAL(AllocatorFlushThresholdSetting),
    DUCKDB_SETTING_CALLBACK(AllowCommunityExtensionsSetting),
    DUCKDB_SETTING(AllowExtensionsMetadataMismatchSetting),
    DUCKDB_SETTING_CALLBACK(AllowParserOverrideExtensionSetting),
    DUCKDB_GLOBAL(AllowPersistentSecretsSetting),
    DUCKDB_SETTING_CALLBACK(AllowUnredactedSecretsSetting),
    DUCKDB_SETTING_CALLBACK(AllowUnsignedExtensionsSetting),
    DUCKDB_GLOBAL(AllowedDirectoriesSetting),
    DUCKDB_GLOBAL(AllowedPathsSetting),
    DUCKDB_SETTING(ArrowLargeBufferSizeSetting),
    DUCKDB_SETTING(ArrowLosslessConversionSetting),
    DUCKDB_SETTING(ArrowOutputListViewSetting),
    DUCKDB_SETTING_CALLBACK(ArrowOutputVersionSetting),
    DUCKDB_SETTING(AsofLoopJoinThresholdSetting),
    DUCKDB_SETTING(AutoCheckpointSkipWalThresholdSetting),
    DUCKDB_SETTING(AutoinstallExtensionRepositorySetting),
    DUCKDB_SETTING(AutoinstallKnownExtensionsSetting),
    DUCKDB_SETTING(AutoloadKnownExtensionsSetting),
    DUCKDB_GLOBAL(BlockAllocatorMemorySetting),
    DUCKDB_SETTING(CatalogErrorMaxSchemasSetting),
    DUCKDB_GLOBAL(CheckpointThresholdSetting),
    DUCKDB_SETTING(CustomExtensionRepositorySetting),
    DUCKDB_LOCAL(CustomProfilingSettingsSetting),
    DUCKDB_GLOBAL(CustomUserAgentSetting),
    DUCKDB_SETTING(DebugAsofIejoinSetting),
    DUCKDB_SETTING_CALLBACK(DebugCheckpointAbortSetting),
    DUCKDB_SETTING(DebugCheckpointSleepMsSetting),
    DUCKDB_SETTING(DebugEvictionQueueSleepMicroSecondsSetting),
    DUCKDB_LOCAL(DebugForceExternalSetting),
    DUCKDB_SETTING(DebugForceNoCrossProductSetting),
    DUCKDB_SETTING_CALLBACK(DebugPhysicalTableScanExecutionStrategySetting),
    DUCKDB_SETTING(DebugSkipCheckpointOnCommitSetting),
    DUCKDB_SETTING(DebugVerifyBlocksSetting),
    DUCKDB_SETTING_CALLBACK(DebugVerifyVectorSetting),
    DUCKDB_SETTING_CALLBACK(DebugWindowModeSetting),
    DUCKDB_SETTING_CALLBACK(DefaultBlockSizeSetting),
    DUCKDB_SETTING_CALLBACK(DefaultCollationSetting),
    DUCKDB_SETTING_CALLBACK(DefaultNullOrderSetting),
    DUCKDB_SETTING_CALLBACK(DefaultOrderSetting),
    DUCKDB_GLOBAL(DefaultSecretStorageSetting),
    DUCKDB_SETTING_CALLBACK(DeprecatedUsingKeySyntaxSetting),
    DUCKDB_SETTING_CALLBACK(DisableDatabaseInvalidationSetting),
    DUCKDB_SETTING(DisableTimestamptzCastsSetting),
    DUCKDB_GLOBAL(DisabledCompressionMethodsSetting),
    DUCKDB_GLOBAL(DisabledFilesystemsSetting),
    DUCKDB_GLOBAL(DisabledLogTypes),
    DUCKDB_GLOBAL(DisabledOptimizersSetting),
    DUCKDB_SETTING_CALLBACK(DuckDBAPISetting),
    DUCKDB_SETTING(DynamicOrFilterThresholdSetting),
    DUCKDB_SETTING_CALLBACK(EnableExternalAccessSetting),
    DUCKDB_SETTING_CALLBACK(EnableExternalFileCacheSetting),
    DUCKDB_SETTING(EnableFSSTVectorsSetting),
    DUCKDB_LOCAL(EnableHTTPLoggingSetting),
    DUCKDB_SETTING(EnableHTTPMetadataCacheSetting),
    DUCKDB_GLOBAL(EnableLogging),
    DUCKDB_SETTING(EnableMacroDependenciesSetting),
    DUCKDB_SETTING(EnableObjectCacheSetting),
    DUCKDB_LOCAL(EnableProfilingSetting),
    DUCKDB_LOCAL(EnableProgressBarSetting),
    DUCKDB_LOCAL(EnableProgressBarPrintSetting),
    DUCKDB_SETTING(EnableViewDependenciesSetting),
    DUCKDB_GLOBAL(EnabledLogTypes),
    DUCKDB_SETTING(ErrorsAsJSONSetting),
    DUCKDB_SETTING(ExperimentalMetadataReuseSetting),
    DUCKDB_SETTING_CALLBACK(ExplainOutputSetting),
    DUCKDB_GLOBAL(ExtensionDirectoriesSetting),
    DUCKDB_SETTING(ExtensionDirectorySetting),
    DUCKDB_SETTING_CALLBACK(ExternalThreadsSetting),
    DUCKDB_SETTING(FileSearchPathSetting),
    DUCKDB_SETTING_CALLBACK(ForceBitpackingModeSetting),
    DUCKDB_SETTING_CALLBACK(ForceCompressionSetting),
    DUCKDB_GLOBAL(ForceMbedtlsUnsafeSetting),
    DUCKDB_GLOBAL(ForceVariantShredding),
    DUCKDB_SETTING(GeometryMinimumShreddingSize),
    DUCKDB_SETTING_CALLBACK(HomeDirectorySetting),
    DUCKDB_LOCAL(HTTPLoggingOutputSetting),
    DUCKDB_SETTING(HTTPProxySetting),
    DUCKDB_SETTING(HTTPProxyPasswordSetting),
    DUCKDB_SETTING(HTTPProxyUsernameSetting),
    DUCKDB_SETTING(IeeeFloatingPointOpsSetting),
    DUCKDB_SETTING(IgnoreUnknownCrsSetting),
    DUCKDB_SETTING(ImmediateTransactionModeSetting),
    DUCKDB_SETTING(IndexScanMaxCountSetting),
    DUCKDB_SETTING_CALLBACK(IndexScanPercentageSetting),
    DUCKDB_SETTING(IntegerDivisionSetting),
    DUCKDB_SETTING_CALLBACK(LambdaSyntaxSetting),
    DUCKDB_SETTING(LateMaterializationMaxRowsSetting),
    DUCKDB_SETTING(LockConfigurationSetting),
    DUCKDB_SETTING_CALLBACK(LogQueryPathSetting),
    DUCKDB_GLOBAL(LoggingLevel),
    DUCKDB_GLOBAL(LoggingMode),
    DUCKDB_GLOBAL(LoggingStorage),
    DUCKDB_SETTING(MaxExecutionTimeSetting),
    DUCKDB_SETTING(MaxExpressionDepthSetting),
    DUCKDB_GLOBAL(MaxMemorySetting),
    DUCKDB_GLOBAL(MaxTempDirectorySizeSetting),
    DUCKDB_SETTING(MaxVacuumTasksSetting),
    DUCKDB_SETTING(MergeJoinThresholdSetting),
    DUCKDB_SETTING(NestedLoopJoinThresholdSetting),
    DUCKDB_SETTING(OldImplicitCastingSetting),
    DUCKDB_LOCAL(OperatorMemoryLimitSetting),
    DUCKDB_SETTING(OrderByNonIntegerLiteralSetting),
    DUCKDB_SETTING_CALLBACK(OrderedAggregateThresholdSetting),
    DUCKDB_SETTING(PartitionedWriteFlushThresholdSetting),
    DUCKDB_SETTING(PartitionedWriteMaxOpenFilesSetting),
    DUCKDB_SETTING(PasswordSetting),
    DUCKDB_SETTING_CALLBACK(PerfectHtThresholdSetting),
    DUCKDB_SETTING_CALLBACK(PinThreadsSetting),
    DUCKDB_SETTING(PivotFilterThresholdSetting),
    DUCKDB_SETTING(PivotLimitSetting),
    DUCKDB_SETTING(PreferRangeJoinsSetting),
    DUCKDB_SETTING(PreserveIdentifierCaseSetting),
    DUCKDB_SETTING(PreserveInsertionOrderSetting),
    DUCKDB_SETTING(ProduceArrowStringViewSetting),
    DUCKDB_LOCAL(ProfileOutputSetting),
    DUCKDB_LOCAL(ProfilingCoverageSetting),
    DUCKDB_LOCAL(ProfilingModeSetting),
    DUCKDB_LOCAL(ProgressBarTimeSetting),
    DUCKDB_SETTING(ScalarSubqueryErrorOnMultipleRowsSetting),
    DUCKDB_SETTING(SchedulerProcessPartialSetting),
    DUCKDB_LOCAL(SchemaSetting),
    DUCKDB_LOCAL(SearchPathSetting),
    DUCKDB_GLOBAL(SecretDirectorySetting),
    DUCKDB_SETTING_CALLBACK(StorageBlockPrefetchSetting),
    DUCKDB_GLOBAL(StorageCompatibilityVersionSetting),
    DUCKDB_LOCAL(StreamingBufferSizeSetting),
    DUCKDB_GLOBAL(TempDirectorySetting),
    DUCKDB_SETTING_CALLBACK(TempFileEncryptionSetting),
    DUCKDB_GLOBAL(ThreadsSetting),
    DUCKDB_SETTING(UsernameSetting),
    DUCKDB_SETTING_CALLBACK(ValidateExternalFileCacheSetting),
    DUCKDB_SETTING(VariantMinimumShreddingSizeSetting),
    DUCKDB_SETTING(WalAutocheckpointEntriesSetting),
    DUCKDB_SETTING_CALLBACK(WarningsAsErrorsSetting),
    DUCKDB_SETTING(WriteBufferRowGroupCountSetting),
    DUCKDB_SETTING(ZstdMinStringLengthSetting),
    FINAL_SETTING};

static const ConfigurationAlias setting_aliases[] = {DUCKDB_SETTING_ALIAS("memory_limit", 97),
                                                     DUCKDB_SETTING_ALIAS("null_order", 40),
                                                     DUCKDB_SETTING_ALIAS("profiling_output", 117),
                                                     DUCKDB_SETTING_ALIAS("user", 132),
                                                     DUCKDB_SETTING_ALIAS("wal_autocheckpoint", 23),
                                                     DUCKDB_SETTING_ALIAS("worker_threads", 131),
                                                     FINAL_ALIAS};

vector<ConfigurationOption> DBConfig::GetOptions() {
	vector<ConfigurationOption> options;
	for (idx_t index = 0; internal_options[index].name; index++) {
		options.push_back(internal_options[index]);
	}
	return options;
}

vector<ConfigurationAlias> DBConfig::GetAliases() {
	vector<ConfigurationAlias> aliases;
	for (idx_t index = 0; index < GetAliasCount(); index++) {
		aliases.push_back(setting_aliases[index]);
	}
	return aliases;
}

SettingCallbackInfo::SettingCallbackInfo(ClientContext &context_p, SetScope scope)
    : config(DBConfig::GetConfig(context_p)), db(context_p.db.get()), context(context_p), scope(scope) {
}

SettingCallbackInfo::SettingCallbackInfo(DBConfig &config, optional_ptr<DatabaseInstance> db)
    : config(config), db(db), context(nullptr), scope(SetScope::GLOBAL) {
}

idx_t DBConfig::GetOptionCount() {
	return sizeof(internal_options) / sizeof(ConfigurationOption) - 1;
}

idx_t DBConfig::GetAliasCount() {
	return sizeof(setting_aliases) / sizeof(ConfigurationAlias) - 1;
}

vector<string> DBConfig::GetOptionNames() {
	vector<string> names;
	for (idx_t index = 0; internal_options[index].name; index++) {
		names.emplace_back(internal_options[index].name);
	}
	for (idx_t index = 0; setting_aliases[index].alias; index++) {
		names.emplace_back(setting_aliases[index].alias);
	}
	return names;
}

optional_ptr<const ConfigurationOption> DBConfig::GetOptionByIndex(idx_t target_index) {
	if (target_index >= GetOptionCount()) {
		return nullptr;
	}
	return internal_options + target_index;
}

optional_ptr<const ConfigurationAlias> DBConfig::GetAliasByIndex(idx_t target_index) {
	if (target_index >= GetAliasCount()) {
		return nullptr;
	}
	return setting_aliases + target_index;
}

optional_ptr<const ConfigurationOption> DBConfig::GetOptionByName(const String &name) {
	auto lname = name.Lower();
	for (idx_t index = 0; internal_options[index].name; index++) {
		D_ASSERT(StringUtil::Lower(internal_options[index].name) == string(internal_options[index].name));
		if (internal_options[index].name == lname) {
			return internal_options + index;
		}
	}
	for (idx_t index = 0; setting_aliases[index].alias; index++) {
		D_ASSERT(StringUtil::Lower(internal_options[index].name) == string(internal_options[index].name));
		if (setting_aliases[index].alias == lname) {
			return GetOptionByIndex(setting_aliases[index].option_index);
		}
	}
	return nullptr;
}

void DBConfig::SetOption(const ConfigurationOption &option, const Value &value) {
	SetOption(nullptr, option, value);
}

void DBConfig::SetOptionByName(const string &name, const Value &value) {
	if (is_user_config) {
		// for user config we just set the option in the `user_options`
		options.user_options[name] = value;
	}
	auto option = DBConfig::GetOptionByName(name);
	if (option) {
		SetOption(*option, value);
		return;
	}

	ExtensionOption extension_option;
	if (TryGetExtensionOption(name, extension_option)) {
		Value target_value = value.DefaultCastAs(extension_option.type);
		SetOption(extension_option.setting_index.GetIndex(), std::move(target_value));
	} else {
		options.unrecognized_options[name] = value;
	}
}

void DBConfig::SetOptionsByName(const case_insensitive_map_t<Value> &values) {
	for (auto &kv : values) {
		auto &name = kv.first;
		auto &value = kv.second;
		SetOptionByName(name, value);
	}
}

void DBConfig::SetOption(optional_ptr<DatabaseInstance> db, const ConfigurationOption &option, const Value &value) {
	Value input = value.DefaultCastAs(ParseLogicalType(option.parameter_type));
	if (option.default_value) {
		// generic option
		if (option.set_callback) {
			SettingCallbackInfo info(*this, db);
			option.set_callback(info, input);
		}
		user_settings.SetUserSetting(option.setting_idx.GetIndex(), std::move(input));
		return;
	}
	if (!option.set_global) {
		throw InvalidInputException("Could not set option \"%s\" as a global option", option.name);
	}
	lock_guard<mutex> guard(config_lock);
	D_ASSERT(option.reset_global);
	option.set_global(db.get(), *this, input);
}

void DBConfig::ResetOption(optional_ptr<DatabaseInstance> db, const ConfigurationOption &option) {
	if (option.default_value) {
		// generic option
		user_settings.ClearSetting(option.setting_idx.GetIndex());
		return;
	}
	if (!option.reset_global) {
		throw InternalException("Could not reset option \"%s\" as a global option", option.name);
	}
	lock_guard<mutex> guard(config_lock);
	D_ASSERT(option.set_global);
	option.reset_global(db.get(), *this);
}

void DBConfig::SetOption(idx_t setting_index, Value value) {
	user_settings.SetUserSetting(setting_index, std::move(value));
}

void DBConfig::SetOption(const string &name, Value value) {
	optional_ptr<const ConfigurationOption> option;
	auto setting_index = TryGetSettingIndex(name, option);
	if (!setting_index.IsValid()) {
		throw InternalException("Unrecognized option %s in DBConfig::SetOption", name);
	}
	SetOption(setting_index.GetIndex(), std::move(value));
}

void DBConfig::ResetOption(const ExtensionOption &extension_option) {
	auto &default_value = extension_option.default_value;
	auto setting_index = extension_option.setting_index.GetIndex();
	if (!default_value.IsNull()) {
		// Default is not NULL, override the setting
		user_settings.SetUserSetting(setting_index, default_value);
	} else {
		// Otherwise just remove it from the 'set_variables' map
		user_settings.ClearSetting(setting_index);
	}
}

void DBConfig::ResetGenericOption(idx_t setting_index) {
	user_settings.ClearSetting(setting_index);
}

LogicalType DBConfig::ParseLogicalType(const string &type) {
	if (StringUtil::EndsWith(type, "[]")) {
		// list - recurse
		auto child_type = ParseLogicalType(type.substr(0, type.size() - 2));
		return LogicalType::LIST(child_type);
	}

	if (StringUtil::EndsWith(type, "]")) {
		// array - recurse
		auto bracket_open_idx = type.rfind('[');
		if (bracket_open_idx == string::npos || bracket_open_idx == 0) {
			throw InternalException("Ill formatted type: '%s'", type);
		}
		idx_t array_size = 0;
		for (auto length_idx = bracket_open_idx + 1; length_idx < type.size() - 1; length_idx++) {
			if (!isdigit(type[length_idx])) {
				throw InternalException("Ill formatted array type: '%s'", type);
			}
			array_size = array_size * 10 + static_cast<idx_t>(type[length_idx] - '0');
		}
		if (array_size == 0 || array_size > ArrayType::MAX_ARRAY_SIZE) {
			throw InternalException("Invalid array size: '%s'", type);
		}
		auto child_type = ParseLogicalType(type.substr(0, bracket_open_idx));
		return LogicalType::ARRAY(child_type, array_size);
	}

	if (StringUtil::StartsWith(type, "MAP(") && StringUtil::EndsWith(type, ")")) {
		// map - recurse
		string map_args = type.substr(4, type.size() - 5);
		vector<string> map_args_vect = StringUtil::SplitWithParentheses(map_args);
		if (map_args_vect.size() != 2) {
			throw InternalException("Ill formatted map type: '%s'", type);
		}
		StringUtil::Trim(map_args_vect[0]);
		StringUtil::Trim(map_args_vect[1]);
		auto key_type = ParseLogicalType(map_args_vect[0]);
		auto value_type = ParseLogicalType(map_args_vect[1]);
		return LogicalType::MAP(key_type, value_type);
	}

	if (StringUtil::StartsWith(type, "UNION(") && StringUtil::EndsWith(type, ")")) {
		// union - recurse
		string union_members_str = type.substr(6, type.size() - 7);
		vector<string> union_members_vect = StringUtil::SplitWithParentheses(union_members_str);
		child_list_t<LogicalType> union_members;
		for (idx_t member_idx = 0; member_idx < union_members_vect.size(); member_idx++) {
			StringUtil::Trim(union_members_vect[member_idx]);
			vector<string> union_member_parts = StringUtil::SplitWithParentheses(union_members_vect[member_idx], ' ');
			if (union_member_parts.size() != 2) {
				throw InternalException("Ill formatted union type: %s", type);
			}
			StringUtil::Trim(union_member_parts[0]);
			StringUtil::Trim(union_member_parts[1]);
			auto value_type = ParseLogicalType(union_member_parts[1]);
			union_members.emplace_back(make_pair(union_member_parts[0], value_type));
		}
		if (union_members.empty() || union_members.size() > UnionType::MAX_UNION_MEMBERS) {
			throw InternalException("Invalid number of union members: '%s'", type);
		}
		return LogicalType::UNION(union_members);
	}

	if (StringUtil::StartsWith(type, "STRUCT(") && StringUtil::EndsWith(type, ")")) {
		// struct - recurse
		string struct_members_str = type.substr(7, type.size() - 8);
		vector<string> struct_members_vect = StringUtil::SplitWithParentheses(struct_members_str);
		child_list_t<LogicalType> struct_members;
		for (idx_t member_idx = 0; member_idx < struct_members_vect.size(); member_idx++) {
			StringUtil::Trim(struct_members_vect[member_idx]);
			vector<string> struct_member_parts = StringUtil::SplitWithParentheses(struct_members_vect[member_idx], ' ');
			if (struct_member_parts.size() != 2) {
				throw InternalException("Ill formatted struct type: %s", type);
			}
			StringUtil::Trim(struct_member_parts[0]);
			StringUtil::Trim(struct_member_parts[1]);
			auto value_type = ParseLogicalType(struct_member_parts[1]);
			struct_members.emplace_back(make_pair(struct_member_parts[0], value_type));
		}
		return LogicalType::STRUCT(struct_members);
	}

	const auto type_id = StringUtil::CIEquals(type, "ANY") ? LogicalTypeId::ANY : TransformStringToLogicalTypeId(type);
	if (type_id == LogicalTypeId::UNBOUND) {
		throw InternalException("Error while generating extension function overloads - unrecognized logical type %s",
		                        type);
	}
	return type_id;
}

bool DBConfig::HasExtensionOption(const string &name) const {
	return user_settings.HasExtensionOption(name);
}

bool DBConfig::TryGetExtensionOption(const String &name, ExtensionOption &result) const {
	return user_settings.TryGetExtensionOption(name, result);
}

void DBConfig::AddExtensionOption(const string &name, string description, LogicalType parameter,
                                  const Value &default_value, set_option_callback_t function, SetScope default_scope) {
	ExtensionOption extension_option(std::move(description), std::move(parameter), function, default_value,
	                                 default_scope);
	auto setting_index = user_settings.AddExtensionOption(name, std::move(extension_option));
	// copy over unrecognized options, if they match the new extension option
	auto iter = options.unrecognized_options.find(name);
	if (iter != options.unrecognized_options.end()) {
		user_settings.SetUserSetting(setting_index, iter->second);
		options.unrecognized_options.erase(iter);
	}
	if (!default_value.IsNull() && !user_settings.IsSet(setting_index)) {
		// Default value is set, insert it into the 'set_variables' list
		user_settings.SetUserSetting(setting_index, default_value);
	}
}

case_insensitive_map_t<ExtensionOption> DBConfig::GetExtensionSettings() const {
	return user_settings.GetExtensionSettings();
}

bool DBConfig::IsInMemoryDatabase(const char *database_path) {
	if (!database_path) {
		// Entirely empty
		return true;
	}
	if (strlen(database_path) == 0) {
		// '' empty string
		return true;
	}
	if (strcmp(database_path, ":memory:") == 0) {
		return true;
	}
	return false;
}

CastFunctionSet &DBConfig::GetCastFunctions() {
	return type_manager->GetCastFunctions();
}

TypeManager &DBConfig::GetTypeManager() {
	return *type_manager;
}

CollationBinding &DBConfig::GetCollationBinding() {
	return *collation_bindings;
}

IndexTypeSet &DBConfig::GetIndexTypes() {
	return *index_types;
}

void DBConfig::SetDefaultMaxMemory() {
	auto memory = GetSystemAvailableMemory(*file_system);
	if (memory == DBConfigOptions().maximum_memory) {
		// If GetSystemAvailableMemory returned the default, use it as is
		options.maximum_memory = memory;
	} else {
		// Otherwise, use 80% of the available memory
		options.maximum_memory = memory * 8 / 10;
	}
}

void DBConfig::SetDefaultTempDirectory() {
	if (!options.use_temporary_directory) {
		options.temporary_directory = string();
	} else if (DBConfig::IsInMemoryDatabase(options.database_path.c_str())) {
		options.temporary_directory = ".tmp";
	} else if (StringUtil::Contains(options.database_path, "?")) {
		options.temporary_directory = StringUtil::Split(options.database_path, "?")[0] + ".tmp";
	} else {
		options.temporary_directory = options.database_path + ".tmp";
	}
}

void DBConfig::CheckLock(const String &name) {
	if (!Settings::Get<LockConfigurationSetting>(*this)) {
		// not locked
		return;
	}
	case_insensitive_set_t allowed_settings {"schema", "search_path"};
	if (allowed_settings.find(name.ToStdString()) != allowed_settings.end()) {
		// we are always allowed to change these settings
		return;
	}
	// not allowed!
	throw InvalidInputException("Cannot change configuration option \"%s\" - the configuration has been locked", name);
}

idx_t DBConfig::GetSystemMaxThreads(FileSystem &fs) {
#ifdef DUCKDB_NO_THREADS
	return 1;
#else
	idx_t physical_cores = std::thread::hardware_concurrency();
#ifdef __linux__
	if (const char *slurm_cpus = getenv("SLURM_CPUS_ON_NODE")) {
		idx_t slurm_threads;
		if (TryCast::Operation<string_t, idx_t>(string_t(slurm_cpus), slurm_threads)) {
			return MaxValue<idx_t>(slurm_threads, 1);
		}
	}
	return MaxValue<idx_t>(CGroups::GetCPULimit(fs, physical_cores), 1);
#else
	return MaxValue<idx_t>(physical_cores, 1);
#endif
#endif
}

idx_t DBConfig::GetSystemAvailableMemory(FileSystem &fs) {
	// System memory detection
	auto memory = FileSystem::GetAvailableMemory();
	auto available_memory = memory.IsValid() ? memory.GetIndex() : DBConfigOptions().maximum_memory;

#ifdef __linux__
	// Check SLURM environment variables first
	const char *slurm_mem_per_node = getenv("SLURM_MEM_PER_NODE");
	const char *slurm_mem_per_cpu = getenv("SLURM_MEM_PER_CPU");

	if (slurm_mem_per_node) {
		auto limit = ParseMemoryLimitSlurm(slurm_mem_per_node);
		if (limit.IsValid()) {
			return limit.GetIndex();
		}
	} else if (slurm_mem_per_cpu) {
		auto mem_per_cpu = ParseMemoryLimitSlurm(slurm_mem_per_cpu);
		if (mem_per_cpu.IsValid()) {
			idx_t num_threads = GetSystemMaxThreads(fs);
			return mem_per_cpu.GetIndex() * num_threads;
		}
	}

	// Check cgroup memory limit
	auto cgroup_memory_limit = CGroups::GetMemoryLimit(fs);
	if (cgroup_memory_limit.IsValid()) {
		auto cgroup_memory_limit_value = cgroup_memory_limit.GetIndex();
		return std::min(cgroup_memory_limit_value, available_memory);
	}
#endif

	return available_memory;
}

idx_t DBConfig::ParseMemoryLimit(const string &arg) {
	if (arg[0] == '-' || arg == "null" || arg == "none") {
		// infinite
		return NumericLimits<idx_t>::Maximum();
	}

	idx_t result;
	string error = StringUtil::TryParseFormattedBytes(arg, result);

	if (!error.empty()) {
		if (error == "Memory cannot be negative") {
			return NumericLimits<idx_t>::Maximum();
		} else {
			throw ParserException(error);
		}
	}

	return result;
}

optional_idx DBConfig::ParseMemoryLimitSlurm(const string &arg) {
	if (arg.empty()) {
		return optional_idx();
	}

	string number_str = arg;
	idx_t multiplier = 1000LL * 1000LL; // Default to MB if no unit specified

	// Check for SLURM-style suffixes
	if (arg.back() == 'K' || arg.back() == 'k') {
		number_str = arg.substr(0, arg.size() - 1);
		multiplier = 1000LL;
	} else if (arg.back() == 'M' || arg.back() == 'm') {
		number_str = arg.substr(0, arg.size() - 1);
		multiplier = 1000LL * 1000LL;
	} else if (arg.back() == 'G' || arg.back() == 'g') {
		number_str = arg.substr(0, arg.size() - 1);
		multiplier = 1000LL * 1000LL * 1000LL;
	} else if (arg.back() == 'T' || arg.back() == 't') {
		number_str = arg.substr(0, arg.size() - 1);
		multiplier = 1000LL * 1000LL * 1000LL * 1000LL;
	}

	// Parse the number
	double limit;
	if (!TryCast::Operation<string_t, double>(string_t(number_str), limit)) {
		return optional_idx();
	}

	if (limit < 0) {
		return static_cast<idx_t>(NumericLimits<int64_t>::Maximum());
	}
	idx_t actual_limit = LossyNumericCast<idx_t>(static_cast<double>(multiplier) * limit);
	if (actual_limit == NumericLimits<idx_t>::Maximum()) {
		return static_cast<idx_t>(NumericLimits<int64_t>::Maximum());
	}
	return actual_limit;
}

// Right now we only really care about access mode when comparing DBConfigs
bool DBConfigOptions::operator==(const DBConfigOptions &other) const {
	return other.access_mode == access_mode && other.user_options == user_options;
}

bool DBConfig::operator==(const DBConfig &other) {
	return other.options == options;
}

bool DBConfig::operator!=(const DBConfig &other) {
	return !(other.options == options);
}

OrderType DBConfig::ResolveOrder(ClientContext &context, OrderType order_type) const {
	if (order_type != OrderType::ORDER_DEFAULT) {
		return order_type;
	}
	return Settings::Get<DefaultOrderSetting>(context);
}

SettingLookupResult DBConfig::TryGetCurrentUserSetting(idx_t setting_index, Value &result) const {
	return user_settings.TryGetSetting(setting_index, result);
}

SettingLookupResult DBConfig::TryGetDefaultValue(optional_ptr<const ConfigurationOption> option, Value &result) {
	if (!option || !option->default_value) {
		return SettingLookupResult();
	}
	auto input_type = ParseLogicalType(option->parameter_type);
	result = Value(option->default_value).DefaultCastAs(input_type);
	return SettingLookupResult(SettingScope::GLOBAL);
}

SettingLookupResult DBConfig::TryGetCurrentSetting(const string &key, Value &result) const {
	optional_ptr<const ConfigurationOption> option;
	auto setting_index = TryGetSettingIndex(key, option);
	if (setting_index.IsValid()) {
		auto lookup_result = TryGetCurrentUserSetting(setting_index.GetIndex(), result);
		if (lookup_result) {
			return lookup_result;
		}
	}
	return TryGetDefaultValue(option, result);
}

optional_idx DBConfig::TryGetSettingIndex(const String &name, optional_ptr<const ConfigurationOption> &option) const {
	ExtensionOption extension_option;
	if (TryGetExtensionOption(name, extension_option)) {
		// extension setting
		return extension_option.setting_index;
	}
	option = GetOptionByName(name);
	if (option) {
		// built-in setting
		return option->setting_idx;
	}
	// unknown setting
	return optional_idx();
}

OrderByNullType DBConfig::ResolveNullOrder(ClientContext &context, OrderType order_type,
                                           OrderByNullType null_type) const {
	if (null_type != OrderByNullType::ORDER_DEFAULT) {
		return null_type;
	}
	auto null_order = Settings::Get<DefaultNullOrderSetting>(context);
	switch (null_order) {
	case DefaultOrderByNullType::NULLS_FIRST:
		return OrderByNullType::NULLS_FIRST;
	case DefaultOrderByNullType::NULLS_LAST:
		return OrderByNullType::NULLS_LAST;
	case DefaultOrderByNullType::NULLS_FIRST_ON_ASC_LAST_ON_DESC:
		return order_type == OrderType::ASCENDING ? OrderByNullType::NULLS_FIRST : OrderByNullType::NULLS_LAST;
	case DefaultOrderByNullType::NULLS_LAST_ON_ASC_FIRST_ON_DESC:
		return order_type == OrderType::ASCENDING ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
	default:
		throw InternalException("Unknown null order setting");
	}
}

string GetDefaultUserAgent() {
	return StringUtil::Format("duckdb/%s(%s)", DuckDB::LibraryVersion(), DuckDB::Platform());
}

const string DBConfig::UserAgent() const {
	auto user_agent = GetDefaultUserAgent();

	auto duckdb_api = Settings::Get<DuckDBAPISetting>(*this);
	if (!duckdb_api.empty()) {
		user_agent += " " + duckdb_api;
	}

	if (!options.custom_user_agent.empty()) {
		user_agent += " " + options.custom_user_agent;
	}
	return user_agent;
}

ExtensionCallbackManager &DBConfig::GetCallbackManager() {
	return *callback_manager;
}

const ExtensionCallbackManager &DBConfig::GetCallbackManager() const {
	return *callback_manager;
}

string DBConfig::SanitizeAllowedPath(const string &path_p) const {
	auto result = file_system->CanonicalizePath(path_p);
	// allowed_directories/allowed_path always uses forward slashes regardless of the OS
	auto path_sep = file_system->PathSeparator(path_p);
	if (path_sep != "/") {
		result = StringUtil::Replace(result, path_sep, "/");
	}
	return result;
}

void DBConfig::AddAllowedDirectory(const string &path) {
	auto allowed_directory = SanitizeAllowedPath(path);
	if (allowed_directory.empty()) {
		throw InvalidInputException("Cannot provide an empty string for allowed_directory");
	}
	// ensure the directory ends with a path separator
	if (!StringUtil::EndsWith(allowed_directory, "/")) {
		allowed_directory += "/";
	}
	options.allowed_directories.insert(allowed_directory);
}

void DBConfig::AddAllowedPath(const string &path) {
	auto allowed_path = SanitizeAllowedPath(path);
	options.allowed_paths.insert(allowed_path);
}

bool DBConfig::CanAccessFile(const string &input_path, FileType type) {
	if (Settings::Get<EnableExternalAccessSetting>(*this)) {
		// all external access is allowed
		return true;
	}
	string path = SanitizeAllowedPath(input_path);

	if (options.allowed_paths.count(path) > 0) {
		// path is explicitly allowed
		return true;
	}

	if (options.allowed_directories.empty()) {
		// no prefix directories specified
		return false;
	}
	if (type == FileType::FILE_TYPE_DIR) {
		// make sure directories end with a /
		if (!StringUtil::EndsWith(path, "/")) {
			path += "/";
		}
	}

	string prefix;
	for (const auto &allowed_directory : options.allowed_directories) {
		if (StringUtil::StartsWith(path, allowed_directory)) {
			prefix = allowed_directory;
			break;
		}
	}

	if (prefix.empty()) {
		// no common prefix found - path is not inside an allowed directory
		return false;
	}
	D_ASSERT(StringUtil::EndsWith(prefix, "/"));
	return true;
}

SerializationOptions::SerializationOptions(AttachedDatabase &db) {
	serialization_compatibility = SerializationCompatibility::FromDatabase(db);
}

SerializationCompatibility SerializationCompatibility::FromDatabase(AttachedDatabase &db) {
	return FromIndex(db.GetStorageManager().GetStorageVersion());
}

SerializationCompatibility SerializationCompatibility::FromIndex(const idx_t version) {
	SerializationCompatibility result;
	result.duckdb_version = "";
	result.serialization_version = version;
	result.manually_set = false;
	return result;
}

SerializationCompatibility SerializationCompatibility::FromString(const string &input) {
	if (input.empty()) {
		throw InvalidInputException("Version string can not be empty");
	}

	auto serialization_version = GetSerializationVersion(input.c_str());
	if (!serialization_version.IsValid()) {
		auto candidates = GetSerializationCandidates();
		throw InvalidInputException("The version string '%s' is not a known DuckDB version, valid options are: %s",
		                            input, StringUtil::Join(candidates, ", "));
	}
	SerializationCompatibility result;
	result.duckdb_version = input;
	result.serialization_version = serialization_version.GetIndex();
	result.manually_set = true;
	return result;
}

SerializationCompatibility SerializationCompatibility::Default() {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	auto res = FromString("latest");
	res.manually_set = false;
	return res;
#else
#ifdef DUCKDB_LATEST_STORAGE
	auto res = FromString("latest");
	res.manually_set = false;
	return res;
#else
	auto res = FromString("v0.10.2");
	res.manually_set = false;
	return res;
#endif
#endif
}

SerializationCompatibility SerializationCompatibility::Latest() {
	auto res = FromString("latest");
	res.manually_set = false;
	return res;
}

bool SerializationCompatibility::Compare(idx_t property_version) const {
	return property_version <= serialization_version;
}

} // namespace duckdb
