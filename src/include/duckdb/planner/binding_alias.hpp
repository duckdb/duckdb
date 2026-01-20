//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/binding_alias.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
class StandardEntry;

struct BindingAlias {
	BindingAlias();
	explicit BindingAlias(string alias);
	BindingAlias(string schema, string alias);
	BindingAlias(string catalog, string schema, string alias);
	explicit BindingAlias(const StandardEntry &entry);

	bool IsSet() const;
	const string &GetAlias() const;

	const string &GetCatalog() const {
		return catalog;
	}
	const string &GetSchema() const {
		return schema;
	}

	bool Matches(const BindingAlias &other) const;
	bool operator==(const BindingAlias &other) const;
	string ToString() const;

private:
	string catalog;
	string schema;
	string alias;
};

} // namespace duckdb
