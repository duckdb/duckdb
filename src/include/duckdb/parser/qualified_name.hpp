//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/qualified_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/planner/binding_alias.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

struct QualifiedName {
	QualifiedName() = default;
	QualifiedName(Identifier catalog_p, Identifier schema_p, Identifier name_p)
	    : catalog(std::move(catalog_p)), schema(std::move(schema_p)), name(std::move(name_p)) {
	}

	const Identifier &Catalog() const {
		return catalog;
	}
	Identifier &Catalog() {
		return catalog;
	}
	const Identifier &Schema() const {
		return schema;
	}
	Identifier &Schema() {
		return schema;
	}
	const Identifier &Name() const {
		return name;
	}
	Identifier &Name() {
		return name;
	}

	//! Parse the (optional) schema and a name from a string in the format of e.g. "schema"."table"; if there is no dot
	//! the schema will be set to INVALID_SCHEMA
	static QualifiedName Parse(const string &input);
	static vector<Identifier> ParseComponents(const string &input);
	string ToString() const;

	hash_t Hash() const;
	bool operator==(const QualifiedName &rhs) const;
	bool operator!=(const QualifiedName &rhs) const;

private:
	Identifier catalog;
	Identifier schema;
	Identifier name;
};

struct QualifiedColumnName {
	QualifiedColumnName();
	QualifiedColumnName(Identifier column_p); // NOLINT: allow implicit conversion from string to column name
	QualifiedColumnName(Identifier table_p, Identifier column_p);
	QualifiedColumnName(const BindingAlias &alias, Identifier column_p);

	Identifier catalog;
	Identifier schema;
	Identifier table;
	Identifier column;

	static QualifiedColumnName Parse(string &input);

	string ToString() const;

	void Serialize(Serializer &serializer) const;
	static QualifiedColumnName Deserialize(Deserializer &deserializer);

	bool IsQualified() const;

	bool operator==(const QualifiedColumnName &rhs) const;
};

} // namespace duckdb
