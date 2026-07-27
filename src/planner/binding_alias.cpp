#include "duckdb/planner/binding_alias.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

//! Build a QualifiedName holding [schema path..., alias], skipping empty schema components
static QualifiedName MakeSchemaAlias(const vector<Identifier> &schema_path, Identifier alias) {
	vector<Identifier> path;
	for (auto &schema : schema_path) {
		// an empty identifier means the schema is absent
		if (!schema.empty()) {
			path.push_back(schema);
		}
	}
	return QualifiedName(std::move(path), std::move(alias));
}

BindingAlias::BindingAlias() {
}

BindingAlias::BindingAlias(Identifier alias_p) : qualified_name(std::move(alias_p)) {
}

BindingAlias::BindingAlias(Identifier schema_p, Identifier alias_p)
    : qualified_name(MakeSchemaAlias({std::move(schema_p)}, std::move(alias_p))) {
}

BindingAlias::BindingAlias(Identifier catalog_p, Identifier schema_p, Identifier alias_p)
    : catalog(std::move(catalog_p)), qualified_name(MakeSchemaAlias({std::move(schema_p)}, std::move(alias_p))) {
}

BindingAlias::BindingAlias(Identifier catalog_p, const vector<Identifier> &schema_path_p, Identifier alias_p)
    : catalog(std::move(catalog_p)), qualified_name(MakeSchemaAlias(schema_path_p, std::move(alias_p))) {
}

BindingAlias::BindingAlias(const StandardEntry &entry)
    : catalog(entry.ParentCatalog().GetName()),
      qualified_name(MakeSchemaAlias(entry.schema.GetSchemaPath(), entry.name)) {
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

vector<Identifier> BindingAlias::GetSchemaPath() const {
	auto &path = qualified_name.Path();
	// the path is [schema path..., alias] - everything but the trailing alias is the schema path
	vector<Identifier> result;
	for (idx_t i = 0; i + 1 < path.size(); i++) {
		result.push_back(path[i]);
	}
	return result;
}

string BindingAlias::ToString() const {
	// QualifiedName::ToString only renders [catalog, schema, name], so we render the full (possibly nested) path here
	string result;
	if (!catalog.empty()) {
		result += SQLIdentifier(catalog);
	}
	for (auto &component : qualified_name.Path()) {
		if (!result.empty()) {
			result += ".";
		}
		result += SQLIdentifier(component);
	}
	return result;
}

bool BindingAlias::Matches(const BindingAlias &other) const {
	// we match based on the specificity of the other (reference) entry
	// i.e. "tbl" matches "catalog.schema.tbl", and "schema2.tbl" matches "catalog.schema1.schema2.tbl"
	// but "schema1.tbl" does not match "catalog.schema1.schema2.tbl" (schema1 is not the immediate schema)
	if (qualified_name.Name() != other.qualified_name.Name()) {
		return false;
	}
	if (!other.catalog.empty()) {
		// the reference specifies a catalog - it must be present and equal
		if (catalog != other.catalog) {
			return false;
		}
	}
	// the reference's schema qualification must be a suffix of this binding's (nested) schema path
	auto &path = qualified_name.Path();
	auto &other_path = other.qualified_name.Path();
	if (other_path.size() > path.size()) {
		return false;
	}
	// compare the schema components, from the immediate schema outwards (the alias itself already matched)
	for (idx_t i = 1; i < other_path.size(); i++) {
		if (path[path.size() - 1 - i] != other_path[other_path.size() - 1 - i]) {
			return false;
		}
	}
	return true;
}

bool BindingAlias::operator==(const BindingAlias &other) const {
	// QualifiedName::operator== only compares [catalog, schema, name], so we compare the full path here
	if (catalog != other.catalog) {
		return false;
	}
	auto &path = qualified_name.Path();
	auto &other_path = other.qualified_name.Path();
	if (path.size() != other_path.size()) {
		return false;
	}
	for (idx_t i = 0; i < path.size(); i++) {
		if (path[i] != other_path[i]) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
