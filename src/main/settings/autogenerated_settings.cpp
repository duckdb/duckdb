#include "duckdb/main/settings.hpp"

//===--------------------------------------------------------------------===//
// Start of the auto-generated list of settings definitions//===--------------------------------------------------------------------===//


//===--------------------------------------------------------------------===//
// Access Mode
//===--------------------------------------------------------------------===//
void AccessModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.access_mode = AccessModeSetting::FromValue(input);
}

void AccessModeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.access_mode = DBConfig().options.access_mode;
}

AccessModeSetting::SETTING_TYPE AccessModeSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return AccessModeSetting::ToValue(config.options.access_mode);
}

//===--------------------------------------------------------------------===//
// Allocator Background Threads
//===--------------------------------------------------------------------===//
void AllocatorBackgroundThreadsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.allocator_background_threads = AllocatorBackgroundThreadsSetting::FromValue(input);
}

void AllocatorBackgroundThreadsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allocator_background_threads = DBConfig().options.allocator_background_threads;
}

AllocatorBackgroundThreadsSetting::SETTING_TYPE AllocatorBackgroundThreadsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return AllocatorBackgroundThreadsSetting::ToValue(config.options.allocator_background_threads);
}

//===--------------------------------------------------------------------===//
// Allocator Flush Threshold
//===--------------------------------------------------------------------===//
void AllocatorFlushThresholdSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.allocator_flush_threshold = AllocatorFlushThresholdSetting::FromValue(input);
}

void AllocatorFlushThresholdSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allocator_flush_threshold = DBConfig().options.allocator_flush_threshold;
}

AllocatorFlushThresholdSetting::SETTING_TYPE AllocatorFlushThresholdSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return AllocatorFlushThresholdSetting::ToValue(config.options.allocator_flush_threshold);
}

//===--------------------------------------------------------------------===//
// Allow Community Extensions
//===--------------------------------------------------------------------===//
void AllowCommunityExtensionsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.allow_community_extensions = AllowCommunityExtensionsSetting::FromValue(input);
}

void AllowCommunityExtensionsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allow_community_extensions = DBConfig().options.allow_community_extensions;
}

AllowCommunityExtensionsSetting::SETTING_TYPE AllowCommunityExtensionsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return AllowCommunityExtensionsSetting::ToValue(config.options.allow_community_extensions);
}

//===--------------------------------------------------------------------===//
// Allow Extensions Metadata Mismatch
//===--------------------------------------------------------------------===//
void AllowExtensionsMetadataMismatchSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.allow_extensions_metadata_mismatch = AllowExtensionsMetadataMismatchSetting::FromValue(input);
}

void AllowExtensionsMetadataMismatchSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allow_extensions_metadata_mismatch = DBConfig().options.allow_extensions_metadata_mismatch;
}

AllowExtensionsMetadataMismatchSetting::SETTING_TYPE AllowExtensionsMetadataMismatchSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return AllowExtensionsMetadataMismatchSetting::ToValue(config.options.allow_extensions_metadata_mismatch);
}

//===--------------------------------------------------------------------===//
// Allow Persistent Secrets
//===--------------------------------------------------------------------===//
void AllowPersistentSecretsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	// *** implement the function *** /
}

void AllowPersistentSecretsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	// *** implement the function *** /
}

AllowPersistentSecretsSetting::SETTING_TYPE AllowPersistentSecretsSetting::GetSetting(const ClientContext &context) {
	// *** implement the function *** /
}

//===--------------------------------------------------------------------===//
// Allow Unredacted Secrets
//===--------------------------------------------------------------------===//
void AllowUnredactedSecretsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.allow_unredacted_secrets = AllowUnredactedSecretsSetting::FromValue(input);
}

void AllowUnredactedSecretsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allow_unredacted_secrets = DBConfig().options.allow_unredacted_secrets;
}

AllowUnredactedSecretsSetting::SETTING_TYPE AllowUnredactedSecretsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return AllowUnredactedSecretsSetting::ToValue(config.options.allow_unredacted_secrets);
}

//===--------------------------------------------------------------------===//
// Allow Unsigned Extensions
//===--------------------------------------------------------------------===//
void AllowUnsignedExtensionsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.allow_unsigned_extensions = AllowUnsignedExtensionsSetting::FromValue(input);
}

void AllowUnsignedExtensionsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allow_unsigned_extensions = DBConfig().options.allow_unsigned_extensions;
}

AllowUnsignedExtensionsSetting::SETTING_TYPE AllowUnsignedExtensionsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return AllowUnsignedExtensionsSetting::ToValue(config.options.allow_unsigned_extensions);
}

//===--------------------------------------------------------------------===//
// Arrow Large Buffer Size
//===--------------------------------------------------------------------===//
void ArrowLargeBufferSizeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.arrow_large_buffer_size = ArrowLargeBufferSizeSetting::FromValue(input);
}

void ArrowLargeBufferSizeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.arrow_large_buffer_size = DBConfig().options.arrow_large_buffer_size;
}

ArrowLargeBufferSizeSetting::SETTING_TYPE ArrowLargeBufferSizeSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ArrowLargeBufferSizeSetting::ToValue(config.options.arrow_large_buffer_size);
}

//===--------------------------------------------------------------------===//
// Arrow Output List View
//===--------------------------------------------------------------------===//
void ArrowOutputListViewSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.arrow_output_list_view = ArrowOutputListViewSetting::FromValue(input);
}

void ArrowOutputListViewSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.arrow_output_list_view = DBConfig().options.arrow_output_list_view;
}

ArrowOutputListViewSetting::SETTING_TYPE ArrowOutputListViewSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ArrowOutputListViewSetting::ToValue(config.options.arrow_output_list_view);
}

//===--------------------------------------------------------------------===//
// Autoinstall Extension Repository
//===--------------------------------------------------------------------===//
void AutoinstallExtensionRepositorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.autoinstall_extension_repository = AutoinstallExtensionRepositorySetting::FromValue(input);
}

void AutoinstallExtensionRepositorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.autoinstall_extension_repository = DBConfig().options.autoinstall_extension_repository;
}

AutoinstallExtensionRepositorySetting::SETTING_TYPE AutoinstallExtensionRepositorySetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return AutoinstallExtensionRepositorySetting::ToValue(config.options.autoinstall_extension_repository);
}

//===--------------------------------------------------------------------===//
// Autoinstall Known Extensions
//===--------------------------------------------------------------------===//
void AutoinstallKnownExtensionsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.autoinstall_known_extensions = AutoinstallKnownExtensionsSetting::FromValue(input);
}

void AutoinstallKnownExtensionsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.autoinstall_known_extensions = DBConfig().options.autoinstall_known_extensions;
}

AutoinstallKnownExtensionsSetting::SETTING_TYPE AutoinstallKnownExtensionsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return AutoinstallKnownExtensionsSetting::ToValue(config.options.autoinstall_known_extensions);
}

//===--------------------------------------------------------------------===//
// Autoload Known Extensions
//===--------------------------------------------------------------------===//
void AutoloadKnownExtensionsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.autoload_known_extensions = AutoloadKnownExtensionsSetting::FromValue(input);
}

void AutoloadKnownExtensionsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.autoload_known_extensions = DBConfig().options.autoload_known_extensions;
}

AutoloadKnownExtensionsSetting::SETTING_TYPE AutoloadKnownExtensionsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return AutoloadKnownExtensionsSetting::ToValue(config.options.autoload_known_extensions);
}

//===--------------------------------------------------------------------===//
// Catalog Error Max Schemas
//===--------------------------------------------------------------------===//
void CatalogErrorMaxSchemasSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.catalog_error_max_schemas = CatalogErrorMaxSchemasSetting::FromValue(input);
}

void CatalogErrorMaxSchemasSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.catalog_error_max_schemas = DBConfig().options.catalog_error_max_schemas;
}

CatalogErrorMaxSchemasSetting::SETTING_TYPE CatalogErrorMaxSchemasSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return CatalogErrorMaxSchemasSetting::ToValue(config.options.catalog_error_max_schemas);
}

//===--------------------------------------------------------------------===//
// Checkpoint Threshold
//===--------------------------------------------------------------------===//
void CheckpointThresholdSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.checkpoint_threshold = CheckpointThresholdSetting::FromValue(input);
}

void CheckpointThresholdSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.checkpoint_threshold = DBConfig().options.checkpoint_threshold;
}

CheckpointThresholdSetting::SETTING_TYPE CheckpointThresholdSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return CheckpointThresholdSetting::ToValue(config.options.checkpoint_threshold);
}

//===--------------------------------------------------------------------===//
// Custom Extension Repository
//===--------------------------------------------------------------------===//
void CustomExtensionRepositorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.custom_extension_repository = CustomExtensionRepositorySetting::FromValue(input);
}

void CustomExtensionRepositorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.custom_extension_repository = DBConfig().options.custom_extension_repository;
}

CustomExtensionRepositorySetting::SETTING_TYPE CustomExtensionRepositorySetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return CustomExtensionRepositorySetting::ToValue(config.options.custom_extension_repository);
}

//===--------------------------------------------------------------------===//
// Custom Profiling Settings
//===--------------------------------------------------------------------===//
void CustomProfilingSettingsSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.custom_profiling_settings = CustomProfilingSettingsSetting::FromValue(input);
}

void CustomProfilingSettingsSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.custom_profiling_settings = ClientConfig().custom_profiling_settings;
}

CustomProfilingSettingsSetting::SETTING_TYPE CustomProfilingSettingsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return CustomProfilingSettingsSetting::ToValue(config.options.custom_profiling_settings);
}

//===--------------------------------------------------------------------===//
// Custom User Agent
//===--------------------------------------------------------------------===//
void CustomUserAgentSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.custom_user_agent = CustomUserAgentSetting::FromValue(input);
}

void CustomUserAgentSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.custom_user_agent = DBConfig().options.custom_user_agent;
}

CustomUserAgentSetting::SETTING_TYPE CustomUserAgentSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return CustomUserAgentSetting::ToValue(config.options.custom_user_agent);
}

//===--------------------------------------------------------------------===//
// Debug Asof Iejoin
//===--------------------------------------------------------------------===//
void DebugAsofIejoinSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.debug_asof_iejoin = DebugAsofIejoinSetting::FromValue(input);
}

void DebugAsofIejoinSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.debug_asof_iejoin = ClientConfig().debug_asof_iejoin;
}

DebugAsofIejoinSetting::SETTING_TYPE DebugAsofIejoinSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DebugAsofIejoinSetting::ToValue(config.options.debug_asof_iejoin);
}

//===--------------------------------------------------------------------===//
// Debug Checkpoint Abort
//===--------------------------------------------------------------------===//
void DebugCheckpointAbortSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.debug_checkpoint_abort = DebugCheckpointAbortSetting::FromValue(input);
}

void DebugCheckpointAbortSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.debug_checkpoint_abort = DBConfig().options.debug_checkpoint_abort;
}

DebugCheckpointAbortSetting::SETTING_TYPE DebugCheckpointAbortSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DebugCheckpointAbortSetting::ToValue(config.options.debug_checkpoint_abort);
}

//===--------------------------------------------------------------------===//
// Debug Force External
//===--------------------------------------------------------------------===//
void DebugForceExternalSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.debug_force_external = DebugForceExternalSetting::FromValue(input);
}

void DebugForceExternalSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.debug_force_external = ClientConfig().debug_force_external;
}

DebugForceExternalSetting::SETTING_TYPE DebugForceExternalSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DebugForceExternalSetting::ToValue(config.options.debug_force_external);
}

//===--------------------------------------------------------------------===//
// Debug Force No Cross Product
//===--------------------------------------------------------------------===//
void DebugForceNoCrossProductSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.debug_force_no_cross_product = DebugForceNoCrossProductSetting::FromValue(input);
}

void DebugForceNoCrossProductSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.debug_force_no_cross_product = ClientConfig().debug_force_no_cross_product;
}

DebugForceNoCrossProductSetting::SETTING_TYPE DebugForceNoCrossProductSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DebugForceNoCrossProductSetting::ToValue(config.options.debug_force_no_cross_product);
}

//===--------------------------------------------------------------------===//
// Debug Window Mode
//===--------------------------------------------------------------------===//
void DebugWindowModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.debug_window_mode = DebugWindowModeSetting::FromValue(input);
}

void DebugWindowModeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.debug_window_mode = DBConfig().options.debug_window_mode;
}

DebugWindowModeSetting::SETTING_TYPE DebugWindowModeSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DebugWindowModeSetting::ToValue(config.options.debug_window_mode);
}

//===--------------------------------------------------------------------===//
// Default Block Size
//===--------------------------------------------------------------------===//
void DefaultBlockSizeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.default_block_size = DefaultBlockSizeSetting::FromValue(input);
}

void DefaultBlockSizeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.default_block_size = DBConfig().options.default_block_size;
}

DefaultBlockSizeSetting::SETTING_TYPE DefaultBlockSizeSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DefaultBlockSizeSetting::ToValue(config.options.default_block_size);
}

//===--------------------------------------------------------------------===//
// Default Collation
//===--------------------------------------------------------------------===//
void DefaultCollationSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.default_collation = DefaultCollationSetting::FromValue(input);
}

void DefaultCollationSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.default_collation = DBConfig().options.default_collation;
}

DefaultCollationSetting::SETTING_TYPE DefaultCollationSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DefaultCollationSetting::ToValue(config.options.default_collation);
}

void DefaultCollationSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.default_collation = DefaultCollationSetting::FromValue(input);
}

void DefaultCollationSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.default_collation = ClientConfig().default_collation;
}

DefaultCollationSetting::SETTING_TYPE DefaultCollationSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DefaultCollationSetting::ToValue(config.options.default_collation);
}

//===--------------------------------------------------------------------===//
// Default Null Order
//===--------------------------------------------------------------------===//
void DefaultNullOrderSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.default_null_order = DefaultNullOrderSetting::FromValue(input);
}

void DefaultNullOrderSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.default_null_order = DBConfig().options.default_null_order;
}

DefaultNullOrderSetting::SETTING_TYPE DefaultNullOrderSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DefaultNullOrderSetting::ToValue(config.options.default_null_order);
}

//===--------------------------------------------------------------------===//
// Default Order
//===--------------------------------------------------------------------===//
void DefaultOrderSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.default_order = DefaultOrderSetting::FromValue(input);
}

void DefaultOrderSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.default_order = DBConfig().options.default_order;
}

DefaultOrderSetting::SETTING_TYPE DefaultOrderSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DefaultOrderSetting::ToValue(config.options.default_order);
}

//===--------------------------------------------------------------------===//
// Default Secret Storage
//===--------------------------------------------------------------------===//
void DefaultSecretStorageSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.default_secret_storage = DefaultSecretStorageSetting::FromValue(input);
}

void DefaultSecretStorageSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.default_secret_storage = DBConfig().options.default_secret_storage;
}

DefaultSecretStorageSetting::SETTING_TYPE DefaultSecretStorageSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DefaultSecretStorageSetting::ToValue(config.options.default_secret_storage);
}

//===--------------------------------------------------------------------===//
// Disabled Filesystems
//===--------------------------------------------------------------------===//
void DisabledFilesystemsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	// *** implement the function *** /
}

void DisabledFilesystemsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	// *** implement the function *** /
}

DisabledFilesystemsSetting::SETTING_TYPE DisabledFilesystemsSetting::GetSetting(const ClientContext &context) {
	// *** implement the function *** /
}

//===--------------------------------------------------------------------===//
// Disabled Optimizers
//===--------------------------------------------------------------------===//
void DisabledOptimizersSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.disabled_optimizers = DisabledOptimizersSetting::FromValue(input);
}

void DisabledOptimizersSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.disabled_optimizers = DBConfig().options.disabled_optimizers;
}

DisabledOptimizersSetting::SETTING_TYPE DisabledOptimizersSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DisabledOptimizersSetting::ToValue(config.options.disabled_optimizers);
}

//===--------------------------------------------------------------------===//
// Duckdb Api
//===--------------------------------------------------------------------===//
void DuckdbApiSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.duckdb_api = DuckdbApiSetting::FromValue(input);
}

void DuckdbApiSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.duckdb_api = DBConfig().options.duckdb_api;
}

DuckdbApiSetting::SETTING_TYPE DuckdbApiSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return DuckdbApiSetting::ToValue(config.options.duckdb_api);
}

//===--------------------------------------------------------------------===//
// Enable External Access
//===--------------------------------------------------------------------===//
void EnableExternalAccessSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.enable_external_access = EnableExternalAccessSetting::FromValue(input);
}

void EnableExternalAccessSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.enable_external_access = DBConfig().options.enable_external_access;
}

EnableExternalAccessSetting::SETTING_TYPE EnableExternalAccessSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return EnableExternalAccessSetting::ToValue(config.options.enable_external_access);
}

//===--------------------------------------------------------------------===//
// Enable Fsst Vectors
//===--------------------------------------------------------------------===//
void EnableFsstVectorsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.enable_fsst_vectors = EnableFsstVectorsSetting::FromValue(input);
}

void EnableFsstVectorsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.enable_fsst_vectors = DBConfig().options.enable_fsst_vectors;
}

EnableFsstVectorsSetting::SETTING_TYPE EnableFsstVectorsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return EnableFsstVectorsSetting::ToValue(config.options.enable_fsst_vectors);
}

//===--------------------------------------------------------------------===//
// Enable Http Logging
//===--------------------------------------------------------------------===//
void EnableHttpLoggingSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_http_logging = EnableHttpLoggingSetting::FromValue(input);
}

void EnableHttpLoggingSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_http_logging = ClientConfig().enable_http_logging;
}

EnableHttpLoggingSetting::SETTING_TYPE EnableHttpLoggingSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return EnableHttpLoggingSetting::ToValue(config.options.enable_http_logging);
}

//===--------------------------------------------------------------------===//
// Enable Http Metadata Cache
//===--------------------------------------------------------------------===//
void EnableHttpMetadataCacheSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.enable_http_metadata_cache = EnableHttpMetadataCacheSetting::FromValue(input);
}

void EnableHttpMetadataCacheSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.enable_http_metadata_cache = DBConfig().options.enable_http_metadata_cache;
}

EnableHttpMetadataCacheSetting::SETTING_TYPE EnableHttpMetadataCacheSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return EnableHttpMetadataCacheSetting::ToValue(config.options.enable_http_metadata_cache);
}

//===--------------------------------------------------------------------===//
// Enable Macro Dependencies
//===--------------------------------------------------------------------===//
void EnableMacroDependenciesSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.enable_macro_dependencies = EnableMacroDependenciesSetting::FromValue(input);
}

void EnableMacroDependenciesSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.enable_macro_dependencies = DBConfig().options.enable_macro_dependencies;
}

EnableMacroDependenciesSetting::SETTING_TYPE EnableMacroDependenciesSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return EnableMacroDependenciesSetting::ToValue(config.options.enable_macro_dependencies);
}

//===--------------------------------------------------------------------===//
// Enable Object Cache
//===--------------------------------------------------------------------===//
void EnableObjectCacheSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.enable_object_cache = EnableObjectCacheSetting::FromValue(input);
}

void EnableObjectCacheSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.enable_object_cache = DBConfig().options.enable_object_cache;
}

EnableObjectCacheSetting::SETTING_TYPE EnableObjectCacheSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return EnableObjectCacheSetting::ToValue(config.options.enable_object_cache);
}

//===--------------------------------------------------------------------===//
// Enable Profiling
//===--------------------------------------------------------------------===//
//===--------------------------------------------------------------------===//
// Enable Progress Bar
//===--------------------------------------------------------------------===//
void EnableProgressBarSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_progress_bar = EnableProgressBarSetting::FromValue(input);
}

void EnableProgressBarSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_progress_bar = ClientConfig().enable_progress_bar;
}

EnableProgressBarSetting::SETTING_TYPE EnableProgressBarSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return EnableProgressBarSetting::ToValue(config.options.enable_progress_bar);
}

//===--------------------------------------------------------------------===//
// Enable Progress Bar Print
//===--------------------------------------------------------------------===//
void EnableProgressBarPrintSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_progress_bar_print = EnableProgressBarPrintSetting::FromValue(input);
}

void EnableProgressBarPrintSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_progress_bar_print = ClientConfig().enable_progress_bar_print;
}

EnableProgressBarPrintSetting::SETTING_TYPE EnableProgressBarPrintSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return EnableProgressBarPrintSetting::ToValue(config.options.enable_progress_bar_print);
}

//===--------------------------------------------------------------------===//
// Enable View Dependencies
//===--------------------------------------------------------------------===//
void EnableViewDependenciesSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.enable_view_dependencies = EnableViewDependenciesSetting::FromValue(input);
}

void EnableViewDependenciesSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.enable_view_dependencies = DBConfig().options.enable_view_dependencies;
}

EnableViewDependenciesSetting::SETTING_TYPE EnableViewDependenciesSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return EnableViewDependenciesSetting::ToValue(config.options.enable_view_dependencies);
}

//===--------------------------------------------------------------------===//
// Errors As Json
//===--------------------------------------------------------------------===//
void ErrorsAsJsonSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.errors_as_json = ErrorsAsJsonSetting::FromValue(input);
}

void ErrorsAsJsonSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.errors_as_json = ClientConfig().errors_as_json;
}

ErrorsAsJsonSetting::SETTING_TYPE ErrorsAsJsonSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ErrorsAsJsonSetting::ToValue(config.options.errors_as_json);
}

//===--------------------------------------------------------------------===//
// Explain Output
//===--------------------------------------------------------------------===//
void ExplainOutputSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.explain_output = ExplainOutputSetting::FromValue(input);
}

void ExplainOutputSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.explain_output = ClientConfig().explain_output;
}

ExplainOutputSetting::SETTING_TYPE ExplainOutputSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ExplainOutputSetting::ToValue(config.options.explain_output);
}

//===--------------------------------------------------------------------===//
// Extension Directory
//===--------------------------------------------------------------------===//
void ExtensionDirectorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.extension_directory = ExtensionDirectorySetting::FromValue(input);
}

void ExtensionDirectorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.extension_directory = DBConfig().options.extension_directory;
}

ExtensionDirectorySetting::SETTING_TYPE ExtensionDirectorySetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ExtensionDirectorySetting::ToValue(config.options.extension_directory);
}

//===--------------------------------------------------------------------===//
// External Threads
//===--------------------------------------------------------------------===//
void ExternalThreadsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.external_threads = ExternalThreadsSetting::FromValue(input);
}

void ExternalThreadsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.external_threads = DBConfig().options.external_threads;
}

ExternalThreadsSetting::SETTING_TYPE ExternalThreadsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ExternalThreadsSetting::ToValue(config.options.external_threads);
}

//===--------------------------------------------------------------------===//
// File Search Path
//===--------------------------------------------------------------------===//
//===--------------------------------------------------------------------===//
// Force Bitpacking Mode
//===--------------------------------------------------------------------===//
void ForceBitpackingModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.force_bitpacking_mode = ForceBitpackingModeSetting::FromValue(input);
}

void ForceBitpackingModeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.force_bitpacking_mode = DBConfig().options.force_bitpacking_mode;
}

ForceBitpackingModeSetting::SETTING_TYPE ForceBitpackingModeSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ForceBitpackingModeSetting::ToValue(config.options.force_bitpacking_mode);
}

//===--------------------------------------------------------------------===//
// Force Compression
//===--------------------------------------------------------------------===//
void ForceCompressionSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.force_compression = ForceCompressionSetting::FromValue(input);
}

void ForceCompressionSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.force_compression = DBConfig().options.force_compression;
}

ForceCompressionSetting::SETTING_TYPE ForceCompressionSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ForceCompressionSetting::ToValue(config.options.force_compression);
}

//===--------------------------------------------------------------------===//
// Home Directory
//===--------------------------------------------------------------------===//
void HomeDirectorySetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.home_directory = HomeDirectorySetting::FromValue(input);
}

void HomeDirectorySetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.home_directory = ClientConfig().home_directory;
}

HomeDirectorySetting::SETTING_TYPE HomeDirectorySetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return HomeDirectorySetting::ToValue(config.options.home_directory);
}

//===--------------------------------------------------------------------===//
// Http Logging Output
//===--------------------------------------------------------------------===//
void HttpLoggingOutputSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.http_logging_output = HttpLoggingOutputSetting::FromValue(input);
}

void HttpLoggingOutputSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.http_logging_output = ClientConfig().http_logging_output;
}

HttpLoggingOutputSetting::SETTING_TYPE HttpLoggingOutputSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return HttpLoggingOutputSetting::ToValue(config.options.http_logging_output);
}

//===--------------------------------------------------------------------===//
// Immediate Transaction Mode
//===--------------------------------------------------------------------===//
void ImmediateTransactionModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.immediate_transaction_mode = ImmediateTransactionModeSetting::FromValue(input);
}

void ImmediateTransactionModeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.immediate_transaction_mode = DBConfig().options.immediate_transaction_mode;
}

ImmediateTransactionModeSetting::SETTING_TYPE ImmediateTransactionModeSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ImmediateTransactionModeSetting::ToValue(config.options.immediate_transaction_mode);
}

//===--------------------------------------------------------------------===//
// Index Scan Max Count
//===--------------------------------------------------------------------===//
void IndexScanMaxCountSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.index_scan_max_count = IndexScanMaxCountSetting::FromValue(input);
}

void IndexScanMaxCountSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.index_scan_max_count = DBConfig().options.index_scan_max_count;
}

IndexScanMaxCountSetting::SETTING_TYPE IndexScanMaxCountSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return IndexScanMaxCountSetting::ToValue(config.options.index_scan_max_count);
}

//===--------------------------------------------------------------------===//
// Index Scan Percentage
//===--------------------------------------------------------------------===//
void IndexScanPercentageSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.index_scan_percentage = IndexScanPercentageSetting::FromValue(input);
}

void IndexScanPercentageSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.index_scan_percentage = DBConfig().options.index_scan_percentage;
}

IndexScanPercentageSetting::SETTING_TYPE IndexScanPercentageSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return IndexScanPercentageSetting::ToValue(config.options.index_scan_percentage);
}

//===--------------------------------------------------------------------===//
// Integer Division
//===--------------------------------------------------------------------===//
void IntegerDivisionSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.integer_division = IntegerDivisionSetting::FromValue(input);
}

void IntegerDivisionSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.integer_division = ClientConfig().integer_division;
}

IntegerDivisionSetting::SETTING_TYPE IntegerDivisionSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return IntegerDivisionSetting::ToValue(config.options.integer_division);
}

//===--------------------------------------------------------------------===//
// Lock Configuration
//===--------------------------------------------------------------------===//
void LockConfigurationSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.lock_configuration = LockConfigurationSetting::FromValue(input);
}

void LockConfigurationSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.lock_configuration = DBConfig().options.lock_configuration;
}

LockConfigurationSetting::SETTING_TYPE LockConfigurationSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return LockConfigurationSetting::ToValue(config.options.lock_configuration);
}

//===--------------------------------------------------------------------===//
// Log Query Path
//===--------------------------------------------------------------------===//
//===--------------------------------------------------------------------===//
// Max Expression Depth
//===--------------------------------------------------------------------===//
void MaxExpressionDepthSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.max_expression_depth = MaxExpressionDepthSetting::FromValue(input);
}

void MaxExpressionDepthSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.max_expression_depth = ClientConfig().max_expression_depth;
}

MaxExpressionDepthSetting::SETTING_TYPE MaxExpressionDepthSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return MaxExpressionDepthSetting::ToValue(config.options.max_expression_depth);
}

//===--------------------------------------------------------------------===//
// Max Memory
//===--------------------------------------------------------------------===//
void MaxMemorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	// *** implement the function *** /
}

void MaxMemorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	// *** implement the function *** /
}

MaxMemorySetting::SETTING_TYPE MaxMemorySetting::GetSetting(const ClientContext &context) {
	// *** implement the function *** /
}

//===--------------------------------------------------------------------===//
// Max Temp Directory Size
//===--------------------------------------------------------------------===//
void MaxTempDirectorySizeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	// *** implement the function *** /
}

void MaxTempDirectorySizeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	// *** implement the function *** /
}

MaxTempDirectorySizeSetting::SETTING_TYPE MaxTempDirectorySizeSetting::GetSetting(const ClientContext &context) {
	// *** implement the function *** /
}

//===--------------------------------------------------------------------===//
// Merge Join Threshold
//===--------------------------------------------------------------------===//
void MergeJoinThresholdSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.merge_join_threshold = MergeJoinThresholdSetting::FromValue(input);
}

void MergeJoinThresholdSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.merge_join_threshold = ClientConfig().merge_join_threshold;
}

MergeJoinThresholdSetting::SETTING_TYPE MergeJoinThresholdSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return MergeJoinThresholdSetting::ToValue(config.options.merge_join_threshold);
}

//===--------------------------------------------------------------------===//
// Nested Loop Join Threshold
//===--------------------------------------------------------------------===//
void NestedLoopJoinThresholdSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.nested_loop_join_threshold = NestedLoopJoinThresholdSetting::FromValue(input);
}

void NestedLoopJoinThresholdSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.nested_loop_join_threshold = ClientConfig().nested_loop_join_threshold;
}

NestedLoopJoinThresholdSetting::SETTING_TYPE NestedLoopJoinThresholdSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return NestedLoopJoinThresholdSetting::ToValue(config.options.nested_loop_join_threshold);
}

//===--------------------------------------------------------------------===//
// Old Implicit Casting
//===--------------------------------------------------------------------===//
void OldImplicitCastingSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.old_implicit_casting = OldImplicitCastingSetting::FromValue(input);
}

void OldImplicitCastingSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.old_implicit_casting = DBConfig().options.old_implicit_casting;
}

OldImplicitCastingSetting::SETTING_TYPE OldImplicitCastingSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return OldImplicitCastingSetting::ToValue(config.options.old_implicit_casting);
}

//===--------------------------------------------------------------------===//
// Ordered Aggregate Threshold
//===--------------------------------------------------------------------===//
void OrderedAggregateThresholdSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.ordered_aggregate_threshold = OrderedAggregateThresholdSetting::FromValue(input);
}

void OrderedAggregateThresholdSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.ordered_aggregate_threshold = ClientConfig().ordered_aggregate_threshold;
}

OrderedAggregateThresholdSetting::SETTING_TYPE OrderedAggregateThresholdSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return OrderedAggregateThresholdSetting::ToValue(config.options.ordered_aggregate_threshold);
}

//===--------------------------------------------------------------------===//
// Partitioned Write Flush Threshold
//===--------------------------------------------------------------------===//
void PartitionedWriteFlushThresholdSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.partitioned_write_flush_threshold = PartitionedWriteFlushThresholdSetting::FromValue(input);
}

void PartitionedWriteFlushThresholdSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.partitioned_write_flush_threshold = ClientConfig().partitioned_write_flush_threshold;
}

PartitionedWriteFlushThresholdSetting::SETTING_TYPE PartitionedWriteFlushThresholdSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return PartitionedWriteFlushThresholdSetting::ToValue(config.options.partitioned_write_flush_threshold);
}

//===--------------------------------------------------------------------===//
// Partitioned Write Max Open Files
//===--------------------------------------------------------------------===//
void PartitionedWriteMaxOpenFilesSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.partitioned_write_max_open_files = PartitionedWriteMaxOpenFilesSetting::FromValue(input);
}

void PartitionedWriteMaxOpenFilesSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.partitioned_write_max_open_files = ClientConfig().partitioned_write_max_open_files;
}

PartitionedWriteMaxOpenFilesSetting::SETTING_TYPE PartitionedWriteMaxOpenFilesSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return PartitionedWriteMaxOpenFilesSetting::ToValue(config.options.partitioned_write_max_open_files);
}

//===--------------------------------------------------------------------===//
// Password
//===--------------------------------------------------------------------===//
void PasswordSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.password = PasswordSetting::FromValue(input);
}

void PasswordSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.password = DBConfig().options.password;
}

PasswordSetting::SETTING_TYPE PasswordSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return PasswordSetting::ToValue(config.options.password);
}

//===--------------------------------------------------------------------===//
// Perfect Ht Threshold
//===--------------------------------------------------------------------===//
void PerfectHtThresholdSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.perfect_ht_threshold = PerfectHtThresholdSetting::FromValue(input);
}

void PerfectHtThresholdSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.perfect_ht_threshold = ClientConfig().perfect_ht_threshold;
}

PerfectHtThresholdSetting::SETTING_TYPE PerfectHtThresholdSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return PerfectHtThresholdSetting::ToValue(config.options.perfect_ht_threshold);
}

//===--------------------------------------------------------------------===//
// Pivot Filter Threshold
//===--------------------------------------------------------------------===//
void PivotFilterThresholdSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.pivot_filter_threshold = PivotFilterThresholdSetting::FromValue(input);
}

void PivotFilterThresholdSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.pivot_filter_threshold = ClientConfig().pivot_filter_threshold;
}

PivotFilterThresholdSetting::SETTING_TYPE PivotFilterThresholdSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return PivotFilterThresholdSetting::ToValue(config.options.pivot_filter_threshold);
}

//===--------------------------------------------------------------------===//
// Pivot Limit
//===--------------------------------------------------------------------===//
void PivotLimitSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.pivot_limit = PivotLimitSetting::FromValue(input);
}

void PivotLimitSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.pivot_limit = ClientConfig().pivot_limit;
}

PivotLimitSetting::SETTING_TYPE PivotLimitSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return PivotLimitSetting::ToValue(config.options.pivot_limit);
}

//===--------------------------------------------------------------------===//
// Prefer Range Joins
//===--------------------------------------------------------------------===//
void PreferRangeJoinsSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.prefer_range_joins = PreferRangeJoinsSetting::FromValue(input);
}

void PreferRangeJoinsSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.prefer_range_joins = ClientConfig().prefer_range_joins;
}

PreferRangeJoinsSetting::SETTING_TYPE PreferRangeJoinsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return PreferRangeJoinsSetting::ToValue(config.options.prefer_range_joins);
}

//===--------------------------------------------------------------------===//
// Preserve Identifier Case
//===--------------------------------------------------------------------===//
void PreserveIdentifierCaseSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.preserve_identifier_case = PreserveIdentifierCaseSetting::FromValue(input);
}

void PreserveIdentifierCaseSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.preserve_identifier_case = ClientConfig().preserve_identifier_case;
}

PreserveIdentifierCaseSetting::SETTING_TYPE PreserveIdentifierCaseSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return PreserveIdentifierCaseSetting::ToValue(config.options.preserve_identifier_case);
}

//===--------------------------------------------------------------------===//
// Preserve Insertion Order
//===--------------------------------------------------------------------===//
void PreserveInsertionOrderSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.preserve_insertion_order = PreserveInsertionOrderSetting::FromValue(input);
}

void PreserveInsertionOrderSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.preserve_insertion_order = DBConfig().options.preserve_insertion_order;
}

PreserveInsertionOrderSetting::SETTING_TYPE PreserveInsertionOrderSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return PreserveInsertionOrderSetting::ToValue(config.options.preserve_insertion_order);
}

//===--------------------------------------------------------------------===//
// Produce Arrow String View
//===--------------------------------------------------------------------===//
void ProduceArrowStringViewSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.produce_arrow_string_view = ProduceArrowStringViewSetting::FromValue(input);
}

void ProduceArrowStringViewSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.produce_arrow_string_view = DBConfig().options.produce_arrow_string_view;
}

ProduceArrowStringViewSetting::SETTING_TYPE ProduceArrowStringViewSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ProduceArrowStringViewSetting::ToValue(config.options.produce_arrow_string_view);
}

//===--------------------------------------------------------------------===//
// Profile Output
//===--------------------------------------------------------------------===//
void ProfileOutputSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.profile_output = ProfileOutputSetting::FromValue(input);
}

void ProfileOutputSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.profile_output = ClientConfig().profile_output;
}

ProfileOutputSetting::SETTING_TYPE ProfileOutputSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ProfileOutputSetting::ToValue(config.options.profile_output);
}

//===--------------------------------------------------------------------===//
// Profiling Mode
//===--------------------------------------------------------------------===//
void ProfilingModeSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.profiling_mode = ProfilingModeSetting::FromValue(input);
}

void ProfilingModeSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.profiling_mode = ClientConfig().profiling_mode;
}

ProfilingModeSetting::SETTING_TYPE ProfilingModeSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ProfilingModeSetting::ToValue(config.options.profiling_mode);
}

//===--------------------------------------------------------------------===//
// Progress Bar Time
//===--------------------------------------------------------------------===//
void ProgressBarTimeSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.progress_bar_time = ProgressBarTimeSetting::FromValue(input);
}

void ProgressBarTimeSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.progress_bar_time = ClientConfig().progress_bar_time;
}

ProgressBarTimeSetting::SETTING_TYPE ProgressBarTimeSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ProgressBarTimeSetting::ToValue(config.options.progress_bar_time);
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void SchemaSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.schema = SchemaSetting::FromValue(input);
}

void SchemaSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.schema = ClientConfig().schema;
}

SchemaSetting::SETTING_TYPE SchemaSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return SchemaSetting::ToValue(config.options.schema);
}

//===--------------------------------------------------------------------===//
// Search Path
//===--------------------------------------------------------------------===//
void SearchPathSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.search_path = SearchPathSetting::FromValue(input);
}

void SearchPathSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.search_path = ClientConfig().search_path;
}

SearchPathSetting::SETTING_TYPE SearchPathSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return SearchPathSetting::ToValue(config.options.search_path);
}

//===--------------------------------------------------------------------===//
// Secret Directory
//===--------------------------------------------------------------------===//
void SecretDirectorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.secret_directory = SecretDirectorySetting::FromValue(input);
}

void SecretDirectorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.secret_directory = DBConfig().options.secret_directory;
}

SecretDirectorySetting::SETTING_TYPE SecretDirectorySetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return SecretDirectorySetting::ToValue(config.options.secret_directory);
}

//===--------------------------------------------------------------------===//
// Storage Compatibility Version
//===--------------------------------------------------------------------===//
void StorageCompatibilityVersionSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.storage_compatibility_version = StorageCompatibilityVersionSetting::FromValue(input);
}

void StorageCompatibilityVersionSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.storage_compatibility_version = DBConfig().options.storage_compatibility_version;
}

StorageCompatibilityVersionSetting::SETTING_TYPE StorageCompatibilityVersionSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return StorageCompatibilityVersionSetting::ToValue(config.options.storage_compatibility_version);
}

//===--------------------------------------------------------------------===//
// Streaming Buffer Size
//===--------------------------------------------------------------------===//
//===--------------------------------------------------------------------===//
// Temp Directory
//===--------------------------------------------------------------------===//
void TempDirectorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.temp_directory = TempDirectorySetting::FromValue(input);
}

void TempDirectorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.temp_directory = DBConfig().options.temp_directory;
}

TempDirectorySetting::SETTING_TYPE TempDirectorySetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return TempDirectorySetting::ToValue(config.options.temp_directory);
}

//===--------------------------------------------------------------------===//
// Threads
//===--------------------------------------------------------------------===//
void ThreadsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.threads = ThreadsSetting::FromValue(input);
}

void ThreadsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.threads = DBConfig().options.threads;
}

ThreadsSetting::SETTING_TYPE ThreadsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return ThreadsSetting::ToValue(config.options.threads);
}

//===--------------------------------------------------------------------===//
// Username
//===--------------------------------------------------------------------===//
void UsernameSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	// *** implement the function *** /
}

void UsernameSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	// *** implement the function *** /
}

UsernameSetting::SETTING_TYPE UsernameSetting::GetSetting(const ClientContext &context) {
	// *** implement the function *** /
}

//===--------------------------------------------------------------------===//
// End of the auto-generated list of settings definitions//===--------------------------------------------------------------------===//
