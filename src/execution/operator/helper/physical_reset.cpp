#include "duckdb/execution/operator/helper/physical_reset.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalReset::ResetExtensionVariable(ClientContext &context, DBConfig &config, ExtensionOption &extension_option,
                                           SetScope scope) {
	auto effective_scope = scope == SetScope::AUTOMATIC ? extension_option.default_scope : scope;
	if (extension_option.set_function) {
		extension_option.set_function(context, effective_scope, extension_option.default_value);
	}
	if (effective_scope == SetScope::GLOBAL) {
		config.ResetOption(extension_option);
	} else {
		auto &client_config = ClientConfig::GetConfig(context);
		auto setting_index = extension_option.setting_index.GetIndex();
		client_config.user_settings.SetUserSetting(setting_index, extension_option.default_value);
	}
}

void PhysicalReset::ResetVariable(ClientContext &context, const Identifier &name, SetScope scope) {
	auto &config = DBConfig::GetConfig(context);
	config.CheckLock(name);
	auto option = DBConfig::GetOptionByName(name);
	if (!option) {
		// check if this is an extra extension variable
		ExtensionOption extension_option;
		if (!config.TryGetExtensionOption(name, extension_option)) {
			auto extension_name = Catalog::AutoloadExtensionByConfigName(context, name);
			if (!config.TryGetExtensionOption(name, extension_option)) {
				throw InvalidInputException("Extension parameter %s was not found after autoloading",
				                            name.GetIdentifierName());
			}
		}
		ResetExtensionVariable(context, config, extension_option, scope);
		return;
	}

	// Transform scope
	SetScope variable_scope = PhysicalSet::GetSettingScope(*option, scope);

	if (option->default_value) {
		if (option->set_callback) {
			SettingCallbackInfo info(context, variable_scope);
			auto parameter_type = DBConfig::ParseLogicalType(option->parameter_type);
			Value reset_val = Value(option->default_value).CastAs(context, parameter_type);
			option->set_callback(info, reset_val);
		}
		auto setting_index = option->setting_idx.GetIndex();
		if (variable_scope == SetScope::SESSION) {
			auto &client_config = ClientConfig::GetConfig(context);
			client_config.user_settings.ClearSetting(setting_index);
		} else {
			config.ResetGenericOption(setting_index);
		}
		return;
	}
	switch (variable_scope) {
	case SetScope::GLOBAL: {
		if (!option->set_global) {
			throw CatalogException("option \"%s\" cannot be reset globally", name.GetIdentifierName());
		}
		auto &db = DatabaseInstance::GetDatabase(context);
		config.ResetOption(&db, *option);
		break;
	}
	case SetScope::SESSION:
		if (!option->reset_local) {
			throw CatalogException("option \"%s\" cannot be reset locally", name.GetIdentifierName());
		}
		option->reset_local(context);
		break;
	default:
		throw InternalException("Unsupported SetScope for variable");
	}
}

SourceResultType PhysicalReset::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	if (scope == SetScope::VARIABLE) {
		auto &client_config = ClientConfig::GetConfig(context.client);
		client_config.ResetUserVariable(name);
		return SourceResultType::FINISHED;
	}
	ResetVariable(context.client, Identifier(name.ToStdString()), scope);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
