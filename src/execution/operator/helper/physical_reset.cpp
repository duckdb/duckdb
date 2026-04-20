#include "duckdb/execution/operator/helper/physical_reset.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

void PhysicalReset::ResetExtensionVariable(ExecutionContext &context, DBConfig &config,
                                           ExtensionOption &extension_option) const {
	// Resolve AUTOMATIC against the option's default_scope first.
	SetScope effective_scope = scope == SetScope::AUTOMATIC ? extension_option.default_scope : scope;

	// Compatibility: mirror the checks from PhysicalSet::SetExtensionVariable.
	if ((extension_option.default_scope == SetScope::GLOBAL && effective_scope != SetScope::GLOBAL) ||
	    (extension_option.default_scope == SetScope::SESSION && effective_scope == SetScope::LOCAL)) {
		throw InvalidInputException("parameter \"%s\" cannot be reset locally", extension_option.description);
	}
	if (effective_scope == SetScope::GLOBAL && !context.client.transaction.IsAutoCommit()) {
		throw InvalidInputException("parameter \"%s\" is global and cannot be reset inside a transaction",
		                            extension_option.description);
	}

	if (extension_option.reset_function) {
		extension_option.reset_function(context.client, effective_scope);
	} else if (extension_option.set_function) {
		extension_option.set_function(context.client, effective_scope, extension_option.default_value);
	}
	if (effective_scope == SetScope::GLOBAL) {
		config.ResetOption(extension_option);
	} else {
		auto &client_config = ClientConfig::GetConfig(context.client);
		auto setting_index = extension_option.setting_index.GetIndex();
		client_config.user_settings.SetUserSetting(setting_index, extension_option.default_value);
	}
}

void PhysicalReset::ResetOption(ExecutionContext &context, DBConfig &config,
                                const ConfigurationOption &option) const {
	config.CheckLock(option.name);
	SetScope variable_scope = PhysicalSet::GetSettingScope(option, scope);

	if (option.default_value) {
		if (option.set_callback) {
			SettingCallbackInfo info(context.client, variable_scope);
			auto parameter_type = DBConfig::ParseLogicalType(option.parameter_type);
			Value reset_val = Value(option.default_value).CastAs(context.client, parameter_type);
			option.set_callback(info, reset_val);
		}
		auto setting_index = option.setting_idx.GetIndex();
		if (variable_scope == SetScope::SESSION || variable_scope == SetScope::LOCAL) {
			auto &client_config = ClientConfig::GetConfig(context.client);
			client_config.user_settings.ClearSetting(setting_index);
		} else {
			config.ResetGenericOption(setting_index);
		}
		return;
	}
	switch (variable_scope) {
	case SetScope::GLOBAL: {
		if (!option.set_global) {
			throw CatalogException("option \"%s\" cannot be reset globally", option.name);
		}
		auto &db = DatabaseInstance::GetDatabase(context.client);
		config.ResetOption(&db, option);
		break;
	}
	case SetScope::LOCAL:
	case SetScope::SESSION:
		if (!option.reset_local) {
			throw CatalogException("option \"%s\" cannot be reset locally", option.name);
		}
		option.reset_local(context.client);
		break;
	default:
		throw InternalException("Unsupported SetScope for variable");
	}
}

SourceResultType PhysicalReset::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	// RESET LOCAL is only meaningful inside a transaction; reject outside.
	if (scope == SetScope::LOCAL && context.client.transaction.IsAutoCommit()) {
		throw InvalidInputException("RESET LOCAL can only be used in transaction blocks");
	}
	if (scope == SetScope::VARIABLE) {
		// SQL user variables live in a separate namespace from configuration
		// options. PhysicalSetVariable does not notify setting_change_handler
		// either, so skip it here for symmetry — the handler is config-option
		// rollback infrastructure, not variable tracking.
		auto &client_config = ClientConfig::GetConfig(context.client);
		client_config.ResetUserVariable(name);
		return SourceResultType::FINISHED;
	}
	auto &config = DBConfig::GetConfig(context.client);
	if (name.empty()) {
		ResetAll(context, config);
		return SourceResultType::FINISHED;
	}

	// Single-target RESET: resolve first, then notify + reset.
	auto option = DBConfig::GetOptionByName(name);
	if (option) {
		if (context.client.setting_change_handler) {
			context.client.setting_change_handler(context.client, option->name, scope);
		}
		ResetOption(context, config, *option);
		return SourceResultType::FINISHED;
	}
	ExtensionOption extension_option;
	if (!config.TryGetExtensionOption(name, extension_option)) {
		Catalog::AutoloadExtensionByConfigName(context.client, name);
		if (!config.TryGetExtensionOption(name, extension_option)) {
			throw InvalidInputException("Extension parameter %s was not found after autoloading", name);
		}
	}
	if (context.client.setting_change_handler) {
		context.client.setting_change_handler(context.client, string(name.data(), name.size()), scope);
	}
	ResetExtensionVariable(context, config, extension_option);
	return SourceResultType::FINISHED;
}

void PhysicalReset::ResetAll(ExecutionContext &context, DBConfig &config) const {
	// RESET ALL / RESET LOCAL ALL: reset every session-level option. Skip
	// GLOBAL-scoped settings — those live DB-wide and PG's RESET ALL does not
	// touch them. Individual resets may throw (readonly/deprecated settings
	// that refuse standalone reset); swallow per-option failures so one bad
	// option doesn't block the rest. TODO: track which options were actually
	// user-set and only reset those.
	auto reset_option = [&](const ConfigurationOption &option) {
		if (context.client.setting_change_handler) {
			context.client.setting_change_handler(context.client, option.name, scope);
		}
		ResetOption(context, config, option);
	};
	auto reset_extension = [&](const String &ext_name, ExtensionOption &ext) {
		if (context.client.setting_change_handler) {
			context.client.setting_change_handler(context.client, string(ext_name.data(), ext_name.size()), scope);
		}
		ResetExtensionVariable(context, config, ext);
	};
	auto options_count = DBConfig::GetOptionCount();
	for (idx_t i = 0; i < options_count; i++) {
		auto option = DBConfig::GetOptionByIndex(i);
		if (!option) {
			continue;
		}
		if (PhysicalSet::GetSettingScope(*option, scope) == SetScope::GLOBAL) {
			continue;
		}
		try {
			reset_option(*option);
		} catch (const std::exception &) {
		}
	}
	for (auto &ext : config.GetExtensionSettings()) {
		if (ext.second.default_scope == SetScope::GLOBAL) {
			continue;
		}
		try {
			reset_extension(ext.first, ext.second);
		} catch (const std::exception &) {
		}
	}
}

} // namespace duckdb
