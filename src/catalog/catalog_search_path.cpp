#include "duckdb/catalog/catalog_search_path.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"

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
	bool finished = false;
normal:
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
	vector<CatalogSearchEntry> empty;
	SetPathsInternal(empty);
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
	if (set_type != CatalogSetPathType::SET_SCHEMAS && new_paths.size() != 1) {
		throw CatalogException("%s can set only 1 schema. This has %d", GetSetName(set_type), new_paths.size());
	}
	for (auto &path : new_paths) {
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
				auto schema = catalog->GetSchema(context, DEFAULT_SCHEMA, OnEntryNotFound::RETURN_NULL);
				if (schema) {
					path.catalog = std::move(path.schema);
					path.schema = schema->name;
					continue;
				}
			}
		}
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

const vector<CatalogSearchEntry> &CatalogSearchPath::Get() {
	return paths;
}

string CatalogSearchPath::GetDefaultSchema(const string &catalog) {
	for (auto &path : paths) {
		if (path.catalog == TEMP_CATALOG) {
			continue;
		}
		if (StringUtil::CIEquals(path.catalog, catalog)) {
			return path.schema;
		}
	}
	return DEFAULT_SCHEMA;
}

string CatalogSearchPath::GetDefaultCatalog(const string &schema) {
	for (auto &path : paths) {
		if (path.catalog == TEMP_CATALOG) {
			continue;
		}
		if (StringUtil::CIEquals(path.schema, schema)) {
			return path.catalog;
		}
	}
	return INVALID_CATALOG;
}

vector<string> CatalogSearchPath::GetCatalogsForSchema(const string &schema) {
	vector<string> schemas;
	for (auto &path : paths) {
		if (StringUtil::CIEquals(path.schema, schema)) {
			schemas.push_back(path.catalog);
		}
	}
	return schemas;
}

vector<string> CatalogSearchPath::GetSchemasForCatalog(const string &catalog) {
	vector<string> schemas;
	for (auto &path : paths) {
		if (StringUtil::CIEquals(path.catalog, catalog)) {
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

void CatalogSearchPath::SetPathsInternal(vector<CatalogSearchEntry> new_paths) {
	this->set_paths = std::move(new_paths);

	paths.clear();
	paths.reserve(set_paths.size() + 3);
	paths.emplace_back(TEMP_CATALOG, DEFAULT_SCHEMA);
	for (auto &path : set_paths) {
		paths.push_back(path);
	}
	paths.emplace_back(INVALID_CATALOG, DEFAULT_SCHEMA);
	paths.emplace_back(SYSTEM_CATALOG, DEFAULT_SCHEMA);
	paths.emplace_back(SYSTEM_CATALOG, "pg_catalog");
}

bool CatalogSearchPath::SchemaInSearchPath(ClientContext &context, const string &catalog_name,
                                           const string &schema_name) {
	for (auto &path : paths) {
		if (!StringUtil::CIEquals(path.schema, schema_name)) {
			continue;
		}
		if (StringUtil::CIEquals(path.catalog, catalog_name)) {
			return true;
		}
		if (IsInvalidCatalog(path.catalog) &&
		    StringUtil::CIEquals(catalog_name, DatabaseManager::GetDefaultDatabase(context))) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
