#include "duckdb/planner/binding_alias.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

BindingAlias::BindingAlias() {
}

BindingAlias::BindingAlias(Identifier alias_p) : qualified_name(std::move(alias_p)) {
}

BindingAlias::BindingAlias(Identifier schema_p, Identifier alias_p)
    : qualified_name(Identifier(), std::move(schema_p), std::move(alias_p)) {
}

BindingAlias::BindingAlias(const StandardEntry &entry)
    : qualified_name(entry.ParentCatalog().GetName(), entry.schema.name, entry.name) {
}

BindingAlias::BindingAlias(Identifier catalog_p, Identifier schema_p, Identifier alias_p)
    : qualified_name(std::move(catalog_p), std::move(schema_p), std::move(alias_p)) {
}

bool BindingAlias::IsSet() const {
	return !qualified_name.Name().empty();
}

const Identifier &BindingAlias::GetAlias() const {
	if (!IsSet()) {
		throw InternalException("Calling BindingAlias::GetAlias on a non-set alias");
	}
	return qualified_name.Name();
}

string BindingAlias::ToString() const {
	return qualified_name.ToString();
}

bool BindingAlias::Matches(const BindingAlias &other) const {
	// we match based on the specificity of the other entry
	// i.e. "tbl" matches "catalog.schema.tbl"
	// but "schema2.tbl" does not match "schema.tbl"
	if (!other.GetCatalog().empty()) {
		if (GetCatalog() != other.GetCatalog()) {
			return false;
		}
	}
	if (!other.GetSchema().empty()) {
		if (GetSchema() != other.GetSchema()) {
			return false;
		}
	}
	return qualified_name.Name() == other.qualified_name.Name();
}

bool BindingAlias::operator==(const BindingAlias &other) const {
	return qualified_name == other.qualified_name;
}

} // namespace duckdb
