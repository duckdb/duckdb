#include "duckdb/execution/operator/helper/physical_set.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalSet::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const {
	D_ASSERT(scope == SetScope::GLOBAL || scope == SetScope::SESSION);

	auto normalized_name = ValidateInput(context);
	if (scope == SetScope::GLOBAL) {
		context.client.db->config.set_variables[normalized_name] = value;
	} else {
		context.client.set_variables[normalized_name] = value;
	}

	state->finished = true;
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

		try {
			for (const auto &path : paths) {
				context.client.db->GetCatalog().GetSchema(context.client, StringUtil::Lower(path));
			}
		} catch (const CatalogException &e) {
			throw CatalogException("SET error: %s", e.what());
		}

		return "search_path";
	}

	return name;
}

} // namespace duckdb
