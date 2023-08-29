#include "duckdb/execution/operator/helper/physical_set.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalSet::SetExtensionVariable(ClientContext &context, ExtensionOption &extension_option, const string &name,
                                       SetScope scope, const Value &value) {
	auto &config = DBConfig::GetConfig(context);
	auto &target_type = extension_option.type;
	Value target_value = value.CastAs(context, target_type);
	if (extension_option.set_function) {
		extension_option.set_function(context, scope, target_value);
	}
	if (scope == SetScope::GLOBAL) {
		config.SetOption(name, std::move(target_value));
	} else {
		auto &client_config = ClientConfig::GetConfig(context);
		client_config.set_variables[name] = std::move(target_value);
	}
}

SourceResultType PhysicalSet::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &config = DBConfig::GetConfig(context.client);
	if (config.options.lock_configuration) {
		throw InvalidInputException("Cannot change configuration option \"%s\" - the configuration has been locked",
		                            name);
	}
	auto option = DBConfig::GetOptionByName(name);
	if (!option) {
		// check if this is an extra extension variable
		auto entry = config.extension_parameters.find(name);
		if (entry == config.extension_parameters.end()) {
			Catalog::AutoloadExtensionByConfigName(context.client, name);
			entry = config.extension_parameters.find(name);
			D_ASSERT(entry != config.extension_parameters.end());
		}
		SetExtensionVariable(context.client, entry->second, name, scope, value);
		return SourceResultType::FINISHED;
	}
	SetScope variable_scope = scope;
	if (variable_scope == SetScope::AUTOMATIC) {
		if (option->set_local) {
			variable_scope = SetScope::SESSION;
		} else {
			D_ASSERT(option->set_global);
			variable_scope = SetScope::GLOBAL;
		}
	}

	Value input_val = value.CastAs(context.client, option->parameter_type);
	switch (variable_scope) {
	case SetScope::GLOBAL: {
		if (!option->set_global) {
			throw CatalogException("option \"%s\" cannot be set globally", name);
		}
		auto &db = DatabaseInstance::GetDatabase(context.client);
		auto &config = DBConfig::GetConfig(context.client);
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
