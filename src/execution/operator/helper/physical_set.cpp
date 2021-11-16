#include "duckdb/execution/operator/helper/physical_set.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalSet::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                          LocalSourceState &lstate) const {
	D_ASSERT(scope == SetScope::GLOBAL || scope == SetScope::SESSION);

	auto normalized_name = ValidateInput(context);
	if (scope == SetScope::GLOBAL) {
		DBConfig::GetConfig(context.client).set_variables[normalized_name] = value;
	} else {
		ClientConfig::GetConfig(context.client).set_variables[normalized_name] = value;
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
