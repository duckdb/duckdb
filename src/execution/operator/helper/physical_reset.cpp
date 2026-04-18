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
	if (extension_option.default_scope == SetScope::GLOBAL) {
		if (scope == SetScope::LOCAL || scope == SetScope::SESSION) {
			throw InvalidInputException("parameter \"%s\" cannot be reset locally", name);
		}
		if (!context.client.transaction.IsAutoCommit()) {
			throw InvalidInputException(
			    "parameter \"%s\" is global and cannot be reset inside a transaction", name);
		}
	} else if (extension_option.default_scope == SetScope::SESSION) {
		if (scope == SetScope::LOCAL) {
			throw InvalidInputException("parameter \"%s\" cannot be reset locally", name);
		}
	}
	auto effective_scope = scope;
	if (effective_scope == SetScope::AUTOMATIC) {
		effective_scope = extension_option.default_scope;
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

SourceResultType PhysicalReset::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	// RESET LOCAL is only meaningful inside a transaction; reject outside.
	if (scope == SetScope::LOCAL && context.client.transaction.IsAutoCommit()) {
		throw InvalidInputException("RESET LOCAL can only be used in transaction blocks");
	}
	if (name.empty()) {
		// RESET ALL / RESET LOCAL ALL: snapshot every user-set setting for
		// rollback tracking, then clear the whole user_settings map.
		auto &client_config = ClientConfig::GetConfig(context.client);
		auto &config = DBConfig::GetConfig(context.client);
		if (context.client.setting_change_handler) {
			auto options_count = DBConfig::GetOptionCount();
			for (idx_t i = 0; i < options_count; i++) {
				if (!client_config.user_settings.IsSet(i)) {
					continue;
				}
				auto option = DBConfig::GetOptionByIndex(i);
				if (!option) {
					continue;
				}
				context.client.setting_change_handler(context.client, option->name, scope);
			}
			for (auto &ext : config.GetExtensionSettings()) {
				if (!ext.second.setting_index.IsValid()) {
					continue;
				}
				if (!client_config.user_settings.IsSet(ext.second.setting_index.GetIndex())) {
					continue;
				}
				context.client.setting_change_handler(context.client, ext.first, scope);
			}
		}
		client_config.user_settings = {};
		return SourceResultType::FINISHED;
	}
	// Pre-reset observer so SereneDB can snapshot the current value before it's
	// cleared — symmetric to the SET path in PhysicalSet::SetVariable.
	if (context.client.setting_change_handler) {
		context.client.setting_change_handler(context.client, string(name.data(), name.size()), scope);
	}
	if (scope == SetScope::VARIABLE) {
		auto &client_config = ClientConfig::GetConfig(context.client);
		client_config.ResetUserVariable(name);
		return SourceResultType::FINISHED;
	}
	auto &config = DBConfig::GetConfig(context.client);
	config.CheckLock(name);
	auto option = DBConfig::GetOptionByName(name);
	if (!option) {
		// check if this is an extra extension variable
		ExtensionOption extension_option;
		if (!config.TryGetExtensionOption(name, extension_option)) {
			auto extension_name = Catalog::AutoloadExtensionByConfigName(context.client, name);
			if (!config.TryGetExtensionOption(name, extension_option)) {
				throw InvalidInputException("Extension parameter %s was not found after autoloading", name);
			}
		}
		ResetExtensionVariable(context, config, extension_option);
		return SourceResultType::FINISHED;
	}

	// Transform scope
	SetScope variable_scope = PhysicalSet::GetSettingScope(*option, scope);

	if (option->default_value) {
		if (option->set_callback) {
			SettingCallbackInfo info(context.client, variable_scope);
			auto parameter_type = DBConfig::ParseLogicalType(option->parameter_type);
			Value reset_val = Value(option->default_value).CastAs(context.client, parameter_type);
			option->set_callback(info, reset_val);
		}
		auto setting_index = option->setting_idx.GetIndex();
		if (variable_scope == SetScope::SESSION || variable_scope == SetScope::LOCAL) {
			auto &client_config = ClientConfig::GetConfig(context.client);
			client_config.user_settings.ClearSetting(setting_index);
		} else {
			config.ResetGenericOption(setting_index);
		}
		return SourceResultType::FINISHED;
	}
	switch (variable_scope) {
	case SetScope::GLOBAL: {
		if (!option->set_global) {
			throw CatalogException("option \"%s\" cannot be reset globally", name.ToStdString());
		}
		auto &db = DatabaseInstance::GetDatabase(context.client);
		config.ResetOption(&db, *option);
		break;
	}
	case SetScope::LOCAL:
	case SetScope::SESSION:
		if (!option->reset_local) {
			throw CatalogException("option \"%s\" cannot be reset locally", name.ToStdString());
		}
		option->reset_local(context.client);
		break;
	default:
		throw InternalException("Unsupported SetScope for variable");
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
