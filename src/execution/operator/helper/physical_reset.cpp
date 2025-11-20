#include "duckdb/execution/operator/helper/physical_reset.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalReset::ResetExtensionVariable(ExecutionContext &context, DBConfig &config,
                                           ExtensionOption &extension_option) const {
	if (extension_option.set_function) {
		extension_option.set_function(context.client, scope, extension_option.default_value);
	}
	if (scope == SetScope::GLOBAL) {
		config.ResetOption(name);
	} else {
		auto &client_config = ClientConfig::GetConfig(context.client);
		client_config.set_variables[name.ToStdString()] = extension_option.default_value;
	}
}

SourceResultType PhysicalReset::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
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
		auto entry = config.extension_parameters.find(name.ToStdString());
		if (entry == config.extension_parameters.end()) {
			auto extension_name = Catalog::AutoloadExtensionByConfigName(context.client, name);
			entry = config.extension_parameters.find(name.ToStdString());
			if (entry == config.extension_parameters.end()) {
				throw InvalidInputException("Extension parameter %s was not found after autoloading", name);
			}
		}
		ResetExtensionVariable(context, config, entry->second);
		return SourceResultType::FINISHED;
	}

	// Transform scope
	SetScope variable_scope = scope;
	if (variable_scope == SetScope::AUTOMATIC) {
		if (option->set_local) {
			variable_scope = SetScope::SESSION;
		} else if (option->set_global) {
			variable_scope = SetScope::GLOBAL;
		} else {
			variable_scope = option->default_scope;
		}
	}

	if (option->default_value) {
		if (option->set_callback) {
			SettingCallbackInfo info(context.client, variable_scope);
			auto parameter_type = DBConfig::ParseLogicalType(option->parameter_type);
			Value reset_val = Value(option->default_value).CastAs(context.client, parameter_type);
			option->set_callback(info, reset_val);
		}
		if (variable_scope == SetScope::SESSION) {
			auto &client_config = ClientConfig::GetConfig(context.client);
			client_config.set_variables.erase(option->name);
		} else {
			config.ResetGenericOption(option->name);
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
