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
		return catalog;
	}
	const Identifier &GetSchema() const {
		return schema;
	}

	bool Matches(const BindingAlias &other) const;
	bool operator==(const BindingAlias &other) const;
	string ToString() const;

private:
	Identifier catalog;
	Identifier schema;
	Identifier alias;
};

} // namespace duckdb
