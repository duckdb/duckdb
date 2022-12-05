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
		config.SetOption(name, extension_option.default_value);
	} else {
		auto &client_config = ClientConfig::GetConfig(context.client);
		client_config.set_variables[name] = extension_option.default_value;
	}
}

void PhysicalReset::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                            LocalSourceState &lstate) const {
	auto option = DBConfig::GetOptionByName(name);
	if (!option) {
		// check if this is an extra extension variable
		auto &config = DBConfig::GetConfig(context.client);
		auto entry = config.extension_parameters.find(name);
		if (entry == config.extension_parameters.end()) {
			// it is not!
			// get a list of all options
			vector<string> potential_names = DBConfig::GetOptionNames();
			for (auto &entry : config.extension_parameters) {
				potential_names.push_back(entry.first);
			}

			throw CatalogException("unrecognized configuration parameter \"%s\"\n%s", name,
			                       StringUtil::CandidatesErrorMessage(potential_names, name, "Did you mean"));
		}
		ResetExtensionVariable(context, config, entry->second);
		return;
	}

	// Transform scope
	SetScope variable_scope = scope;
	if (variable_scope == SetScope::AUTOMATIC) {
		if (option->set_local) {
			variable_scope = SetScope::SESSION;
		} else {
			D_ASSERT(option->set_global);
			variable_scope = SetScope::GLOBAL;
		}
	}

	switch (variable_scope) {
	case SetScope::GLOBAL: {
		if (!option->set_global) {
			throw CatalogException("option \"%s\" cannot be reset globally", name);
		}
		auto &db = DatabaseInstance::GetDatabase(context.client);
		auto &config = DBConfig::GetConfig(context.client);
		config.ResetOption(&db, *option);
		break;
	}
	case SetScope::SESSION:
		if (!option->reset_local) {
			throw CatalogException("option \"%s\" cannot be reset locally", name);
		}
		option->reset_local(context.client);
		break;
	default:
		throw InternalException("Unsupported SetScope for variable");
	}
}

} // namespace duckdb
