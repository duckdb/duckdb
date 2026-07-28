//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/binding_alias.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {
class StandardEntry;

struct BindingAlias {
	BindingAlias();
	explicit BindingAlias(Identifier alias);
	BindingAlias(Identifier schema, Identifier alias);
	BindingAlias(Identifier catalog, Identifier schema, Identifier alias);
	//! Construct from an explicit catalog + (possibly nested) schema path + alias
	BindingAlias(Identifier catalog, const vector<Identifier> &schema_path, Identifier alias);
	explicit BindingAlias(const StandardEntry &entry);

	bool IsSet() const;
	const Identifier &GetAlias() const;

	//! The catalog, or an empty identifier if none was specified
	const Identifier &GetCatalog() const {
		return catalog;
	}
	//! The immediate (innermost) schema, or an empty identifier if there is none
	const Identifier &GetSchema() const {
		return qualified_name.Schema();
	}
	//! The full (possibly nested) schema path, outermost schema first (empty if unqualified)
	vector<Identifier> GetSchemaPath() const;

	//! Whether the (possibly less specific) `other` reference matches this (fully-qualified) binding alias
	bool Matches(const BindingAlias &other) const;
	bool operator==(const BindingAlias &other) const;
	string ToString() const;

private:
	//! The catalog (empty if unqualified)
	Identifier catalog;
	//! The (possibly nested) schema path followed by the alias: [schema path..., alias]
	QualifiedName qualified_name;
};

} // namespace duckdb
