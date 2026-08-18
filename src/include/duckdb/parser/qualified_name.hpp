//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/qualified_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/binding_alias.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

struct QualifiedName {
	QualifiedName() = default;
	//! NOTE(backport): DuckDB 2.0 stores the components as a single `vector<Identifier> path` behind accessors, in
	//! preparation for multi-level schemas, and its constructors and accessors deal in `Identifier`. On this branch
	//! catalog/schema/name are plain strings everywhere, so they stay strings here too - `Identifier` converts
	//! implicitly to and from `string` on this branch, so 2.0 call sites that pass an Identifier still compile.
	//! `Path()`/`WithQualification()` are not provided, since there is no path to expose.
	//! Construct an unqualified name (no catalog/schema). Implicit so that a name can be passed wherever an
	//! unqualified QualifiedName lookup is expected.
	QualifiedName(string name_p) // NOLINT: allow implicit conversion
	    : catalog(INVALID_CATALOG), schema(INVALID_SCHEMA), name(std::move(name_p)) {
	}
	QualifiedName(string catalog_p, string schema_p, string name_p)
	    : catalog(std::move(catalog_p)), schema(std::move(schema_p)), name(std::move(name_p)) {
	}

	string catalog;
	string schema;
	string name;

	const string &Catalog() const {
		return catalog;
	}
	const string &Schema() const {
		return schema;
	}
	const string &Name() const {
		return name;
	}

	//! Return a copy of this name with the name replaced, keeping the catalog/schema qualification
	QualifiedName WithName(string name_p) const {
		return QualifiedName(catalog, schema, std::move(name_p));
	}
	//! Return a copy of this name qualified with the given catalog, replacing the catalog it already has
	QualifiedName WithCatalog(string catalog_p) const {
		return QualifiedName(std::move(catalog_p), schema, name);
	}
	//! Drop the catalog component (if any), keeping the schema and the name
	void StripCatalog() {
		catalog = INVALID_CATALOG;
	}

	bool operator==(const QualifiedName &rhs) const {
		return StringUtil::CIEquals(catalog, rhs.catalog) && StringUtil::CIEquals(schema, rhs.schema) &&
		       StringUtil::CIEquals(name, rhs.name);
	}
	bool operator!=(const QualifiedName &rhs) const {
		return !(*this == rhs);
	}

	//! Parse the (optional) schema and a name from a string in the format of e.g. "schema"."table"; if there is no dot
	//! the schema will be set to INVALID_SCHEMA
	static QualifiedName Parse(const string &input);
	static vector<string> ParseComponents(const string &input);
	string ToString() const;
};

struct QualifiedColumnName {
	QualifiedColumnName();
	QualifiedColumnName(string column_p); // NOLINT: allow implicit conversion from string to column name
	QualifiedColumnName(string table_p, string column_p);
	QualifiedColumnName(const BindingAlias &alias, string column_p);

	string catalog;
	string schema;
	string table;
	string column;

	static QualifiedColumnName Parse(string &input);

	string ToString() const;

	void Serialize(Serializer &serializer) const;
	static QualifiedColumnName Deserialize(Deserializer &deserializer);

	bool IsQualified() const;

	bool operator==(const QualifiedColumnName &rhs) const;
};

} // namespace duckdb
