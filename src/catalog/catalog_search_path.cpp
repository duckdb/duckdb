#include "duckdb/catalog/catalog_search_path.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CatalogSearchPath::CatalogSearchPath(ClientContext &context_p) : context(context_p) {
	SetPaths(ParsePaths(""));
}

void CatalogSearchPath::Set(vector<string> &new_paths, bool is_set_schema) {
	if (is_set_schema && new_paths.size() != 1) {
		throw CatalogException("SET schema can set only 1 schema. This has %d", new_paths.size());
	}
	auto &catalog = Catalog::GetCatalog(context);
	for (const auto &path : new_paths) {
		if (!catalog.GetSchema(context, StringUtil::Lower(path), true)) {
			throw CatalogException("SET %s: No schema named %s found.", is_set_schema ? "schema" : "search_path", path);
		}
	}
	this->set_paths = move(new_paths);
	SetPaths(set_paths);
}

void CatalogSearchPath::Set(const string &new_value, bool is_set_schema) {
	auto new_paths = ParsePaths(new_value);
	Set(new_paths, is_set_schema);
}

const vector<string> &CatalogSearchPath::Get() {
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

void CatalogSearchPath::SetPaths(vector<string> new_paths) {
	paths.clear();
	paths.reserve(new_paths.size() + 3);
	paths.emplace_back(TEMP_SCHEMA);
	for (auto &path : new_paths) {
		paths.push_back(move(path));
	}
	paths.emplace_back(DEFAULT_SCHEMA);
	paths.emplace_back("pg_catalog");
}

vector<string> CatalogSearchPath::ParsePaths(const string &value) {
	return StringUtil::SplitWithQuote(StringUtil::Lower(value));
}

} // namespace duckdb
