#include "duckdb/catalog/catalog_search_path.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CatalogSearchEntry::CatalogSearchEntry(string catalog_p, string schema_p)
    : catalog(move(catalog_p)), schema(move(schema_p)) {
}

string CatalogSearchEntry::ToString() const {
	if (catalog.empty()) {
		return WriteOptionallyQuoted(schema);
	} else {
		return WriteOptionallyQuoted(catalog) + "." + WriteOptionallyQuoted(schema);
	}
}

string CatalogSearchEntry::WriteOptionallyQuoted(const string &input) {
	for (idx_t i = 0; i < input.size(); i++) {
		if (input[i] == '.' || input[i] == ',') {
			return "\"" + input + "\"";
		}
	}
	return input;
}

string CatalogSearchEntry::ListToString(const vector<CatalogSearchEntry> &input) {
	string result;
	for (auto &entry : input) {
		if (!result.empty()) {
			result += ",";
		}
		result += entry.ToString();
	}
	return result;
}

CatalogSearchEntry CatalogSearchEntry::ParseInternal(const string &input, idx_t &idx) {
	string catalog;
	string schema;
	string entry;
normal:
	for (; idx < input.size(); idx++) {
		if (input[idx] == '"') {
			idx++;
			goto quoted;
		} else if (input[idx] == '.') {
			goto separator;
		} else if (input[idx] == ',') {
			idx++;
			goto final;
		}
		entry += input[idx];
	}
	goto final;
quoted:
	//! look for another quote
	for (; idx < input.size(); idx++) {
		if (input[idx] == '"') {
			//! unquote
			idx++;
			goto normal;
		}
		entry += input[idx];
	}
	throw ParserException("Unterminated quote in qualified name!");
separator:
	if (entry.empty()) {
		throw ParserException("Unexpected dot - empty CatalogSearchEntry");
	}
	if (schema.empty()) {
		// if we parse one entry it is the schema
		schema = move(entry);
	} else if (catalog.empty()) {
		// if we parse two entries it is [catalog.schema]
		catalog = move(schema);
		schema = move(entry);
	} else {
		throw ParserException("Too many dots - expected [schema] or [catalog.schema] for CatalogSearchEntry");
	}
	entry = "";
	idx++;
	goto normal;
final:
	if (schema.empty()) {
		throw ParserException("Unexpected end of entry - empty CatalogSearchEntry");
	}
	return CatalogSearchEntry(move(catalog), move(schema));
}

CatalogSearchEntry CatalogSearchEntry::Parse(const string &input) {
	idx_t pos = 0;
	auto result = ParseInternal(input, pos);
	if (pos != input.size()) {
		throw ParserException("Failed to convert entry \"%s\" to CatalogSearchEntry", input);
	}
	return result;
}

vector<CatalogSearchEntry> CatalogSearchEntry::ParseList(const string &input) {
	idx_t pos = 0;
	vector<CatalogSearchEntry> result;
	while (pos < input.size()) {
		auto entry = ParseInternal(input, pos);
		result.push_back(entry);
	}
	return result;
}

CatalogSearchPath::CatalogSearchPath(ClientContext &context_p) : context(context_p) {
	vector<CatalogSearchEntry> empty;
	SetPaths(empty);
}

void CatalogSearchPath::Set(const vector<CatalogSearchEntry> &new_paths, bool is_set_schema) {
	if (is_set_schema && new_paths.size() != 1) {
		throw CatalogException("SET schema can set only 1 schema. This has %d", new_paths.size());
	}
	for (const auto &path : new_paths) {
		if (!Catalog::GetSchema(context, path.catalog, path.schema, true)) {
			throw CatalogException("SET %s: No catalog + schema named %s found.",
			                       is_set_schema ? "schema" : "search_path", path.ToString());
		}
	}
	this->set_paths = move(new_paths);
	SetPaths(set_paths);
}

void CatalogSearchPath::Set(const CatalogSearchEntry &new_value, bool is_set_schema) {
	vector<CatalogSearchEntry> new_paths {new_value};
	Set(new_paths, is_set_schema);
}

const vector<CatalogSearchEntry> &CatalogSearchPath::Get() {
	return paths;
}

string CatalogSearchPath::GetDefaultSchema(const string &catalog) {
	for (auto &path : paths) {
		if (path.catalog == catalog) {
			return path.schema;
		}
	}
	return DEFAULT_SCHEMA;
}

string CatalogSearchPath::GetDefaultCatalog(const string &schema) {
	for (auto &path : paths) {
		if (path.schema == schema) {
			return path.catalog;
		}
	}
	return INVALID_CATALOG;
}

vector<string> CatalogSearchPath::GetCatalogsForSchema(const string &schema) {
	vector<string> schemas;
	for (auto &path : paths) {
		if (path.schema == schema) {
			schemas.push_back(path.catalog);
		}
	}
	return schemas;
}

vector<string> CatalogSearchPath::GetSchemasForCatalog(const string &catalog) {
	vector<string> schemas;
	for (auto &path : paths) {
		if (path.catalog == catalog) {
			schemas.push_back(path.schema);
		}
	}
	return schemas;
}

const CatalogSearchEntry &CatalogSearchPath::GetDefault() {
	const auto &paths = Get();
	D_ASSERT(paths.size() >= 2);
	return paths[1];
}

void CatalogSearchPath::SetPaths(vector<CatalogSearchEntry> new_paths) {
	paths.clear();
	paths.reserve(new_paths.size() + 3);
	paths.emplace_back(TEMP_CATALOG, DEFAULT_SCHEMA);
	for (auto &path : new_paths) {
		paths.push_back(move(path));
	}
	paths.emplace_back(INVALID_CATALOG, DEFAULT_SCHEMA);
	paths.emplace_back(SYSTEM_CATALOG, DEFAULT_SCHEMA);
	paths.emplace_back(SYSTEM_CATALOG, "pg_catalog");
}

} // namespace duckdb
