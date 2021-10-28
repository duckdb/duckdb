#include "duckdb/catalog/catalog_search_path.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

CatalogSearchPath::CatalogSearchPath(ClientContext &context_p) : context(context_p), paths(ParsePaths("")) {
}

const vector<string> &CatalogSearchPath::Get() {
	Value value;
	context.TryGetCurrentSetting("search_path", value);
	if (value.str_value != last_value) {
		paths = ParsePaths(value.str_value);
		last_value = value.str_value;
	}

	return paths;
}

const string &CatalogSearchPath::GetOrDefault(const string &name) {
	return name == INVALID_SCHEMA ? GetDefault() : name; // NOLINT
}

const string &CatalogSearchPath::GetDefault() {
	const auto &paths = Get();
	D_ASSERT(paths.size() >= 2);
	D_ASSERT(paths[0] == TEMP_SCHEMA);
	return paths[1];
}

vector<string> CatalogSearchPath::ParsePaths(const string &value) {
	vector<string> paths;
	paths.emplace_back(TEMP_SCHEMA);

	auto given_paths = StringUtil::SplitWithQuote(value);
	for (const auto &p : given_paths) {
		paths.emplace_back(StringUtil::Lower(p));
	}

	paths.emplace_back(DEFAULT_SCHEMA);
	paths.emplace_back("pg_catalog");
	return paths;
}

} // namespace duckdb
