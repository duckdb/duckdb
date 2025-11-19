#include "duckdb/execution/operator/helper/physical_set.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalSet::SetGenericVariable(ClientContext &context, const String &name, SetScope scope, Value target_value) {
	if (scope == SetScope::GLOBAL) {
		auto &config = DBConfig::GetConfig(context);
		config.SetOption(name, std::move(target_value));
	} else {
		auto &client_config = ClientConfig::GetConfig(context);
		client_config.set_variables[name.ToStdString()] = std::move(target_value);
	}
}

void PhysicalSet::SetExtensionVariable(ClientContext &context, ExtensionOption &extension_option, const String &name,
                                       SetScope scope, const Value &value) {
	auto &target_type = extension_option.type;
	Value target_value = value.CastAs(context, target_type);
	if (extension_option.set_function) {
		extension_option.set_function(context, scope, target_value);
	}
	if (scope == SetScope::AUTOMATIC) {
		scope = extension_option.default_scope;
	}
	SetGenericVariable(context, name, scope, std::move(target_value));
}

SourceResultType PhysicalSet::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &config = DBConfig::GetConfig(context.client);
	// check if we are allowed to change the configuration option
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
		SetExtensionVariable(context.client, entry->second, name, scope, value);
		return SourceResultType::FINISHED;
	}
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

	Value input_val = value.CastAs(context.client, DBConfig::ParseLogicalType(option->parameter_type));
	if (option->default_value) {
		if (option->set_callback) {
			SettingCallbackInfo info(context.client, variable_scope);
			option->set_callback(info, input_val);
		}
		SetGenericVariable(context.client, option->name, variable_scope, std::move(input_val));
		return SourceResultType::FINISHED;
	}
	switch (variable_scope) {
	case SetScope::GLOBAL: {
		if (!option->set_global) {
			throw CatalogException("option \"%s\" cannot be set globally", name);
		}
		auto &db = DatabaseInstance::GetDatabase(context.client);
		config.SetOption(&db, *option, input_val);
		break;
	}
	case SetScope::SESSION:
		if (!option->set_local) {
			throw CatalogException("option \"%s\" cannot be set locally", name);
		}
		option->set_local(context.client, input_val);
		break;
	default:
		throw InternalException("Unsupported SetScope for variable");
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
