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
			auto closest_settings = StringUtil::TopNLevenshtein(potential_names, name);
			throw CatalogException("unrecognized configuration parameter \"%s\"\n%s", name,
			                       StringUtil::CandidatesMessage(closest_settings, "Did you mean"));
		}
		//! it is!
		auto &target_type = entry->second.type;
		if (scope == SetScope::GLOBAL) {
			config.set_variables[name] = value.CastAs(target_type);
		} else {
			ClientConfig::GetConfig(context.client).set_variables[name] = value.CastAs(target_type);
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
