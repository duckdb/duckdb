#include "duckdb/execution/operator/helper/physical_set.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalSet::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                          LocalSourceState &lstate) const {
	auto option = DBConfig::GetOptionByName(name);
	if (!option) {
		// check if this is an extra extension variable
		auto &config = DBConfig::GetConfig(context.client);
		auto entry = config.extension_parameters.find(name);
		if (entry == config.extension_parameters.end()) {
			// it is not!
			// get a list of all options
			vector<string> potential_names;
			for (idx_t i = 0, option_count = DBConfig::GetOptionCount(); i < option_count; i++) {
				potential_names.emplace_back(DBConfig::GetOptionByIndex(i)->name);
			}
			for (auto &entry : config.extension_parameters) {
				potential_names.push_back(entry.first);
			}

			throw CatalogException("unrecognized configuration parameter \"%s\"\n%s", name,
			                       StringUtil::CandidatesErrorMessage(potential_names, name, "Did you mean"));
		}
		//! it is!
		auto &extension_option = entry->second;
		auto &target_type = extension_option.type;
		Value target_value = value.CastAs(target_type);
		if (extension_option.set_function) {
			extension_option.set_function(context.client, scope, target_value);
		}
		if (scope == SetScope::GLOBAL) {
			config.options.set_variables[name] = move(target_value);
		} else {
			auto &client_config = ClientConfig::GetConfig(context.client);
			client_config.set_variables[name] = move(target_value);
		}
		return;
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

	Value input = value.CastAs(option->parameter_type);
	switch (variable_scope) {
	case SetScope::GLOBAL: {
		if (!option->set_global) {
			throw CatalogException("option \"%s\" cannot be set globally", name);
		}
		auto &db = DatabaseInstance::GetDatabase(context.client);
		auto &config = DBConfig::GetConfig(context.client);
		option->set_global(&db, config, input);
		break;
	}
	case SetScope::SESSION:
		if (!option->set_local) {
			throw CatalogException("option \"%s\" cannot be set locally", name);
		}
		option->set_local(context.client, input);
		break;
	default:
		throw InternalException("Unsupported SetScope for variable");
	}
}

} // namespace duckdb
