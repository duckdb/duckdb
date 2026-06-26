//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/binding_alias.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {
class StandardEntry;

struct BindingAlias {
	BindingAlias();
	explicit BindingAlias(Identifier alias);
	BindingAlias(Identifier schema, Identifier alias);
	BindingAlias(Identifier catalog, Identifier schema, Identifier alias);
	explicit BindingAlias(const StandardEntry &entry);

	bool IsSet() const;
	const Identifier &GetAlias() const;

	const Identifier &GetCatalog() const {
		return qualified_name.Catalog();
	}
	const Identifier &GetSchema() const {
		return qualified_name.Schema();
	}

	bool Matches(const BindingAlias &other) const;
	bool operator==(const BindingAlias &other) const;
	string ToString() const;

private:
	QualifiedName qualified_name;
};

} // namespace duckdb
