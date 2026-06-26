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
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct QualifiedName {
	QualifiedName() = default;
	QualifiedName(Identifier catalog_p, Identifier schema_p, Identifier name_p) : name(std::move(name_p)) {
		// store the catalog/schema as a schema path - in preparation for multi-level schema support
		// for now we only support a single schema level, so the path is at most [catalog, schema]
		if (!catalog_p.empty()) {
			schema_path.push_back(std::move(catalog_p));
			schema_path.push_back(std::move(schema_p));
		} else if (!schema_p.empty()) {
			schema_path.push_back(std::move(schema_p));
		}
	}

	//! The catalog is the first element of the schema path, but only when the path is fully qualified (size 2)
	const Identifier &Catalog() const {
		return schema_path.size() == 2 ? schema_path[0] : empty;
	}
	Identifier &CatalogMutable() {
		EnsureQualified();
		return schema_path[0];
	}
	//! The schema is the last element of the schema path (or empty if there is no schema)
	const Identifier &Schema() const {
		if (schema_path.size() == 1) {
			return schema_path[0];
		}
		if (schema_path.size() == 2) {
			return schema_path[1];
		}
		return empty;
	}
	Identifier &SchemaMutable() {
		EnsureQualified();
		return schema_path[1];
	}
	const Identifier &Name() const {
		return name;
	}
	Identifier &NameMutable() {
		return name;
	}
	//! The full schema path of the qualified name (catalog/schema components)
	const vector<Identifier> &SchemaPath() const {
		return schema_path;
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
	//! Normalize the schema path to be fully qualified ([catalog, schema]) so that CatalogMutable()/SchemaMutable()
	//! return stable references - the catalog lives at [0] and the schema at [1]
	void EnsureQualified() {
		if (schema_path.empty()) {
			schema_path.resize(2);
		} else if (schema_path.size() == 1) {
			schema_path.insert(schema_path.begin(), Identifier());
		}
	}

private:
	//! The schema path (catalog/schema). For now at most [catalog, schema] (single schema level).
	vector<Identifier> schema_path;
	//! The name of the entry
	Identifier name;
	//! Always-empty identifier, returned by the accessors when a catalog/schema component is absent
	Identifier empty;
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
