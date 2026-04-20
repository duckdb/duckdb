#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"

#include "duckdb/common/exception/parser_exception.hpp"
#include <algorithm>

namespace duckdb {

CatalogSearchEntry::CatalogSearchEntry(string catalog_p, string schema_p)
    : catalog(std::move(catalog_p)), schema(std::move(schema_p)) {
}

string CatalogSearchEntry::ToString() const {
	if (catalog.empty()) {
		return WriteOptionallyQuoted(schema);
	} else {
		return WriteOptionallyQuoted(catalog) + "." + WriteOptionallyQuoted(schema);
	}
}

string CatalogSearchEntry::WriteOptionallyQuoted(const string &input) {
	// PG-compliant: always quote the "$user" placeholder so it is visually
	// distinct from a schema literally named $user.
	if (input == "$user") {
		return "\"$user\"";
	}
	// PG-compliant: leave unquoted only if the identifier is either a run of digits
	// or a lower-case identifier [a-z_][a-z0-9_]*. Anything else (uppercase,
	// leading digit followed by letters, `.`, `,`, `"`, `$`, spaces, ...)
	// needs quoting.
	bool needs_quote = input.empty();
	if (!needs_quote) {
		const bool all_digits = std::all_of(input.begin(), input.end(), [](char c) { return c >= '0' && c <= '9'; });
		if (!all_digits) {
			char first = input.front();
			if (!((first >= 'a' && first <= 'z') || first == '_')) {
				needs_quote = true;
			} else {
				for (idx_t i = 1; i < input.size() && !needs_quote; i++) {
					char c = input[i];
					if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) {
						needs_quote = true;
					}
				}
			}
		}
	}
	if (needs_quote) {
		return "\"" + StringUtil::Replace(input, "\"", "\"\"") + "\"";
	}
	return input;
}

string CatalogSearchEntry::ListToString(const vector<CatalogSearchEntry> &input) {
	string result;
	for (auto &entry : input) {
		if (!result.empty()) {
			result += ", ";
		}
		result += entry.ToString();
	}
	return result;
}

CatalogSearchEntry CatalogSearchEntry::ParseInternal(const string &input, idx_t &idx) {
	string catalog;
	string schema;
	string entry;
	bool finished = false;
normal:
	// PG-compliant: skip leading whitespace (e.g. after a comma in a list).
	while (idx < input.size() && entry.empty() && StringUtil::CharacterIsSpace(input[idx])) {
		idx++;
	}
	for (; idx < input.size(); idx++) {
		if (input[idx] == '"') {
			idx++;
			goto quoted;
		} else if (input[idx] == '.') {
			goto separator;
		} else if (input[idx] == ',') {
			finished = true;
			goto separator;
		}
		entry += input[idx];
	}
	finished = true;
	goto separator;
quoted:
	//! look for another quote
	for (; idx < input.size(); idx++) {
		if (input[idx] == '"') {
			//! unquote
			idx++;
			if (idx < input.size() && input[idx] == '"') {
				// escaped quote
				entry += input[idx];
				continue;
			}
			goto normal;
		}
		entry += input[idx];
	}
	throw ParserException("Unterminated quote in qualified name!");
separator:
	// Trim trailing whitespace for unquoted identifiers.
	while (!entry.empty() && StringUtil::CharacterIsSpace(entry.back())) {
		entry.pop_back();
	}
	if (entry.empty()) {
		throw ParserException("Unexpected dot - empty CatalogSearchEntry");
	}
	if (schema.empty()) {
		// if we parse one entry it is the schema
		schema = std::move(entry);
	} else if (catalog.empty()) {
		// if we parse two entries it is [catalog.schema]
		catalog = std::move(schema);
		schema = std::move(entry);
	} else {
		throw ParserException("Too many dots - expected [schema] or [catalog.schema] for CatalogSearchEntry");
	}
	entry = "";
	idx++;
	if (finished) {
		goto final;
	}
	goto normal;
final:
	if (schema.empty()) {
		throw ParserException("Unexpected end of entry - empty CatalogSearchEntry");
	}
	return CatalogSearchEntry(std::move(catalog), std::move(schema));
}

CatalogSearchEntry CatalogSearchEntry::Parse(const string &input) {
	idx_t pos = 0;
	auto result = ParseInternal(input, pos);
	if (pos < input.size()) {
		throw ParserException("Failed to convert entry \"%s\" to CatalogSearchEntry - expected a single entry", input);
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

CatalogSearchPath::CatalogSearchPath(ClientContext &context_p, vector<CatalogSearchEntry> entries)
    : context(context_p) {
	SetPathsInternal(std::move(entries));
}

CatalogSearchPath::CatalogSearchPath(ClientContext &context_p) : CatalogSearchPath(context_p, {}) {
}

void CatalogSearchPath::Reset() {
	// Restore to the connection-level defaults (empty if never configured).
	SetPathsInternal(default_paths);
}

void CatalogSearchPath::SetDefaultPaths(vector<CatalogSearchEntry> new_defaults) {
	for (auto &entry : new_defaults) {
		if (entry.catalog.empty() || entry.schema.empty()) {
			throw InternalException("SetDefaultPaths requires fully qualified entries");
		}
	}
	default_paths = std::move(new_defaults);
}

string CatalogSearchPath::GetSetName(CatalogSetPathType set_type) {
	switch (set_type) {
	case CatalogSetPathType::SET_SCHEMA:
		return "SET schema";
	case CatalogSetPathType::SET_SCHEMAS:
		return "SET search_path";
	default:
		throw InternalException("Unrecognized CatalogSetPathType");
	}
}

void CatalogSearchPath::Set(vector<CatalogSearchEntry> new_paths, CatalogSetPathType set_type) {
	if (set_type == CatalogSetPathType::SET_SCHEMA && new_paths.size() != 1) {
		throw CatalogException("%s can set only 1 schema. This has %d", GetSetName(set_type), new_paths.size());
	}
	for (auto &path : new_paths) {
		if (set_type == CatalogSetPathType::SET_DIRECTLY) {
			if (path.catalog.empty() || path.schema.empty()) {
				throw InternalException("SET_WITHOUT_VERIFICATION requires a fully qualified set path");
			}
			continue;
		}
		auto schema_entry = Catalog::GetSchema(context, path.catalog, path.schema, OnEntryNotFound::RETURN_NULL);
		if (schema_entry) {
			// we are setting a schema - update the catalog and schema
			if (path.catalog.empty()) {
				path.catalog = GetDefault().catalog;
			}
			continue;
		}
		// only schema supplied - check if this is a catalog instead
		if (path.catalog.empty()) {
			auto catalog = Catalog::GetCatalogEntry(context, path.schema);
			if (catalog) {
				auto schema = catalog->GetSchema(context, catalog->GetDefaultSchema(), OnEntryNotFound::RETURN_NULL);
				if (schema) {
					path.catalog = std::move(path.schema);
					path.schema = schema->name;
					continue;
				}
			}
			// PG-compliant: silently accept unknown schemas. Default the catalog
			// to the current DB; later lookups against the missing schema will
			// just be skipped during search-path resolution.
			path.catalog = GetDefault().catalog;
			continue;
		}
		// Catalog was explicitly specified and not found — keep the strict
		// error: an invalid catalog short-circuits all unqualified lookups
		throw CatalogException("%s: No catalog + schema named \"%s\" found.", GetSetName(set_type), path.ToString());
	}
	if (set_type == CatalogSetPathType::SET_SCHEMA) {
		if (new_paths[0].catalog == TEMP_CATALOG || new_paths[0].catalog == SYSTEM_CATALOG) {
			throw CatalogException("%s cannot be set to internal schema \"%s\"", GetSetName(set_type),
			                       new_paths[0].catalog);
		}
	}
	SetPathsInternal(std::move(new_paths));
}

void CatalogSearchPath::Set(CatalogSearchEntry new_value, CatalogSetPathType set_type) {
	vector<CatalogSearchEntry> new_paths {std::move(new_value)};
	Set(std::move(new_paths), set_type);
}

// Resolves the literal "$user" placeholder to the current session user.
// Returns empty when the entry has no schema, or when it is "$user" but no
// session user is set — caller should skip such entries.
static string ResolveSchema(ClientContext &context, const CatalogSearchEntry &entry) {
	if (entry.schema.empty()) {
		return {};
	}
	if (entry.schema == "$user") {
		return context.session_user;
	}
	return entry.schema;
}

vector<CatalogSearchEntry> CatalogSearchPath::Get() const {
	vector<CatalogSearchEntry> res;
	res.reserve(paths.size());
	for (auto &path : paths) {
		auto resolved = ResolveSchema(context, path);
		if (resolved.empty()) {
			continue;
		}
		res.emplace_back(path.catalog, std::move(resolved));
	}
	return res;
}

vector<CatalogSearchEntry> CatalogSearchPath::GetResolvedSetPaths() const {
	vector<CatalogSearchEntry> res;
	res.reserve(set_paths.size());
	for (auto &path : set_paths) {
		auto resolved = ResolveSchema(context, path);
		if (resolved.empty()) {
			continue;
		}
		res.emplace_back(path.catalog, std::move(resolved));
	}
	return res;
}

string CatalogSearchPath::GetDefaultSchema(const string &catalog) const {
	for (auto &path : paths) {
		if (path.catalog == TEMP_CATALOG) {
			continue;
		}
		if (StringUtil::CIEquals(path.catalog, catalog)) {
			auto resolved = ResolveSchema(context, path);
			if (resolved.empty()) {
				continue;
			}
			return resolved;
		}
	}
	return DEFAULT_SCHEMA;
}

string CatalogSearchPath::GetDefaultSchema(ClientContext &context_p, const string &catalog) const {
	for (auto &path : paths) {
		if (path.catalog == TEMP_CATALOG) {
			continue;
		}
		if (StringUtil::CIEquals(path.catalog, catalog)) {
			auto resolved = ResolveSchema(context_p, path);
			if (resolved.empty()) {
				continue;
			}
			return resolved;
		}
	}
	auto catalog_entry = Catalog::GetCatalogEntry(context_p, catalog);
	if (catalog_entry) {
		return catalog_entry->GetDefaultSchema();
	}
	return DEFAULT_SCHEMA;
}

string CatalogSearchPath::GetDefaultCatalog(const string &schema) const {
	if (DefaultSchemaGenerator::IsDefaultSchema(schema)) {
		// Check attached catalogs first -- they may override system schemas
		// (e.g. SereneDB serves its own pg_catalog/information_schema)
		for (auto &path : paths) {
			if (path.catalog == TEMP_CATALOG || path.catalog == SYSTEM_CATALOG || path.catalog.empty()) {
				continue;
			}
			return path.catalog;
		}
		return SYSTEM_CATALOG;
	}
	for (auto &path : paths) {
		if (path.catalog == TEMP_CATALOG) {
			continue;
		}
		auto resolved = ResolveSchema(context, path);
		if (resolved.empty()) {
			continue;
		}
		if (StringUtil::CIEquals(resolved, schema)) {
			return path.catalog;
		}
	}
	return INVALID_CATALOG;
}

vector<string> CatalogSearchPath::GetCatalogsForSchema(const string &schema) const {
	vector<string> catalogs;
	if (DefaultSchemaGenerator::IsDefaultSchema(schema)) {
		// Check attached catalogs first, system catalog as fallback.
		// This lets attached catalogs (e.g. SereneDB) serve pg_catalog/information_schema
		// while keeping DuckDB's versions accessible via explicit system.pg_catalog.*
		for (auto &path : paths) {
			if (path.catalog == TEMP_CATALOG || path.catalog == SYSTEM_CATALOG || path.catalog.empty()) {
				continue;
			}
			catalogs.emplace_back(path.catalog);
		}
		catalogs.emplace_back(SYSTEM_CATALOG);
	} else {
		catalogs.reserve(paths.size());
		for (auto &path : paths) {
			if (path.schema.empty()) {
				catalogs.push_back(path.catalog);
				continue;
			}
			auto resolved = ResolveSchema(context, path);
			if (resolved.empty()) {
				continue;
			}
			if (StringUtil::CIEquals(resolved, schema)) {
				catalogs.push_back(path.catalog);
			}
		}
	}
	return catalogs;
}

vector<string> CatalogSearchPath::GetSchemasForCatalog(const string &catalog) const {
	vector<string> schemas;
	schemas.reserve(paths.size());
	for (auto &path : paths) {
		if (!StringUtil::CIEquals(path.catalog, catalog)) {
			continue;
		}
		auto resolved = ResolveSchema(context, path);
		if (resolved.empty()) {
			continue;
		}
		schemas.emplace_back(std::move(resolved));
	}
	return schemas;
}

const CatalogSearchEntry &CatalogSearchPath::GetDefault() const {
	D_ASSERT(paths.size() >= 2);
	return paths[1];
}

CatalogSearchEntry CatalogSearchPath::GetResolvedDefault() const {
	D_ASSERT(paths.size() >= set_paths.size() + 2);
	// No user-set entries -> no default schema (PG: current_schema is NULL,
	// CREATE without schema prefix errors with "no schema has been selected").
	if (set_paths.empty()) {
		return {"", ""};
	}
	// Walk the user-set range and return the first entry whose schema resolves
	// AND actually exists in the target catalog. PG falls through to the next
	// search_path entry when "$user" doesn't match a real schema.
	auto user_end = 1 + set_paths.size();
	for (idx_t i = 1; i < user_end; i++) {
		auto resolved = ResolveSchema(context, paths[i]);
		if (resolved.empty()) {
			continue;
		}
		auto schema = Catalog::GetSchema(context, paths[i].catalog, resolved, OnEntryNotFound::RETURN_NULL);
		if (schema) {
			return {paths[i].catalog, std::move(resolved)};
		}
	}
	return paths[1];
}

void CatalogSearchPath::SetPathsInternal(vector<CatalogSearchEntry> new_paths) {
	this->set_paths = std::move(new_paths);

	paths.clear();
	paths.reserve(set_paths.size() + 4);
	paths.emplace_back(TEMP_CATALOG, DEFAULT_SCHEMA);
	for (auto &path : set_paths) {
		paths.push_back(path);
	}
	paths.emplace_back(INVALID_CATALOG, DEFAULT_SCHEMA);
	paths.emplace_back(SYSTEM_CATALOG, DEFAULT_SCHEMA);
	paths.emplace_back(INVALID_CATALOG, "pg_catalog");
	paths.emplace_back(SYSTEM_CATALOG, "pg_catalog");
}

bool CatalogSearchPath::SchemaInSearchPath(ClientContext &context, const string &catalog_name,
                                           const string &schema_name) const {
	for (auto &path : paths) {
		auto resolved = ResolveSchema(context, path);
		if (resolved.empty() || !StringUtil::CIEquals(resolved, schema_name)) {
			continue;
		}
		bool catalog_matches = StringUtil::CIEquals(path.catalog, catalog_name) ||
		                       (IsInvalidCatalog(path.catalog) &&
		                        StringUtil::CIEquals(catalog_name, DatabaseManager::GetDefaultDatabase(context)));
		if (!catalog_matches) {
			continue;
		}
		// PG-compliant: silently-accepted invalid entries (set via SET
		// search_path = 'nonexistent') are effectively ignored — confirm the
		// schema actually exists before reporting it as in the path.
		auto schema_entry = Catalog::GetSchema(context, catalog_name, schema_name, OnEntryNotFound::RETURN_NULL);
		if (schema_entry) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
