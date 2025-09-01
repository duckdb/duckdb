#include "duckdb/planner/binding_alias.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

BindingAlias::BindingAlias() {
}

BindingAlias::BindingAlias(string alias_p) : alias(std::move(alias_p)) {
}

BindingAlias::BindingAlias(string schema_p, string alias_p) : schema(std::move(schema_p)), alias(std::move(alias_p)) {
}

BindingAlias::BindingAlias(const StandardEntry &entry)
    : catalog(entry.ParentCatalog().GetName()), schema(entry.schema.name), alias(entry.name) {
}

BindingAlias::BindingAlias(string catalog_p, string schema_p, string alias_p)
    : catalog(std::move(catalog_p)), schema(std::move(schema_p)), alias(std::move(alias_p)) {
}

bool BindingAlias::IsSet() const {
	return !alias.empty();
}

const string &BindingAlias::GetAlias() const {
	if (!IsSet()) {
		throw InternalException("Calling BindingAlias::GetAlias on a non-set alias");
	}
	return alias;
}

string BindingAlias::ToString() const {
	string result;
	if (!catalog.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(catalog) + ".";
	}
	if (!schema.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
	}
	result += KeywordHelper::WriteOptionallyQuoted(alias);
	return result;
}

bool BindingAlias::Matches(const BindingAlias &other) const {
	// we match based on the specificity of the other entry
	// i.e. "tbl" matches "catalog.schema.tbl"
	// but "schema2.tbl" does not match "schema.tbl"
	if (!other.catalog.empty()) {
		if (!StringUtil::CIEquals(catalog, other.catalog)) {
			return false;
		}
	}
	if (!other.schema.empty()) {
		if (!StringUtil::CIEquals(schema, other.schema)) {
			return false;
		}
	}
	return StringUtil::CIEquals(alias, other.alias);
}

bool BindingAlias::operator==(const BindingAlias &other) const {
	return StringUtil::CIEquals(catalog, other.catalog) && StringUtil::CIEquals(schema, other.schema) &&
	       StringUtil::CIEquals(alias, other.alias);
}

} // namespace duckdb
