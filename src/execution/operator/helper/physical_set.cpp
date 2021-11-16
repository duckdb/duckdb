#include "duckdb/execution/operator/helper/physical_set.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalSet::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                          LocalSourceState &lstate) const {
	auto option = DBConfig::GetOptionByName(name);
	if (!option) {
		throw CatalogException("unrecognized configuration parameter \"%s\"", name);
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
	switch(variable_scope) {
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

string PhysicalSet::ValidateInput(ExecutionContext &context) const {
	CaseInsensitiveStringEquality case_insensitive_streq;
	if (case_insensitive_streq(name, "search_path") || case_insensitive_streq(name, "schema")) {
		auto paths = StringUtil::SplitWithQuote(value.str_value, ',');

		// The PG doc says:
		// >  SET SCHEMA 'value' is an alias for SET search_path TO value.
		// >  Only one schema can be specified using this syntax.
		if (case_insensitive_streq(name, "schema") && paths.size() > 1) {
			throw CatalogException("SET schema can set only 1 schema. This has %d", paths.size());
		}

		for (const auto &path : paths) {
			if (!context.client.db->GetCatalog().GetSchema(context.client, StringUtil::Lower(path), true)) {
				throw CatalogException("SET %s: No schema named %s found.", name, path);
			}
		}

		return "search_path";
	}

	return name;
}

} // namespace duckdb
