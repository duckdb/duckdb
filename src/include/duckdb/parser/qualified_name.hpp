//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/qualified_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
struct BindingAlias;
class Serializer;
class Deserializer;

//! Controls how QualifiedName::ToString renders the schema qualification
enum class QualifiedNameToStringMode : uint8_t {
	//! Always render every (non-empty) component
	DEFAULT,
	//! Omit the schema when it is a system schema (the default/"main" schema)
	HIDE_DEFAULT_SCHEMA
};

struct QualifiedName {
	QualifiedName() = default;
	//! Construct an unqualified name (no catalog/schema). Implicit so that an Identifier can be passed wherever an
	//! unqualified QualifiedName lookup is expected (also preserves backwards-compatibility for extensions).
	QualifiedName(Identifier name_p) { // NOLINT: allow implicit conversion
		path.push_back(std::move(name_p));
	}
	QualifiedName(Identifier catalog_p, Identifier schema_p, Identifier name_p) {
		// store the catalog/schema/name as a single path - in preparation for multi-level schema support
		// for now we only support a single schema level, so the path is at most [catalog, schema, name]
		if (!catalog_p.empty()) {
			path.push_back(std::move(catalog_p));
			path.push_back(std::move(schema_p));
		} else if (!schema_p.empty()) {
			path.push_back(std::move(schema_p));
		}
		path.push_back(std::move(name_p));
	}
	//! Construct from an explicit schema path (the catalog/schema components actually present) and a name. Use this to
	//! avoid passing INVALID_CATALOG/INVALID_SCHEMA placeholders for components that are not set.
	QualifiedName(vector<Identifier> schema_path_p, Identifier name_p) : path(std::move(schema_path_p)) {
		path.push_back(std::move(name_p));
	}

	//! The catalog is the first element of the path, but only when the path is fully qualified ([catalog, schema,
	//! name])
	const Identifier &Catalog() const {
		return path.size() == 3 ? path[0] : empty;
	}
	//! The schema is the element directly before the name (or empty if there is no schema)
	const Identifier &Schema() const {
		return path.size() >= 2 ? path[path.size() - 2] : empty;
	}
	//! The name is the last element of the path
	const Identifier &Name() const {
		return path.empty() ? empty : path.back();
	}
	//! The full underlying path. Most callers should use Catalog()/Schema()/Name(); this is for multi-level schema
	//! support (e.g. nested CREATE SCHEMA), where the qualification can be deeper than [catalog, schema].
	const vector<Identifier> &Path() const {
		return path;
	}

	//! Return a copy of this name with the name replaced, keeping the catalog/schema qualification
	QualifiedName WithName(Identifier name) const {
		return QualifiedName(Catalog(), Schema(), std::move(name));
	}
	//! Return a copy of this name with the catalog/schema qualification replaced by the given path, keeping the name
	QualifiedName WithQualification(vector<Identifier> schema_path) const {
		return QualifiedName(std::move(schema_path), Name());
	}

	//! Parse the (optional) schema and a name from a string in the format of e.g. "schema"."table"; if there is no dot
	//! the schema will be set to INVALID_SCHEMA
	static QualifiedName Parse(const string &input);
	static vector<Identifier> ParseComponents(const string &input);
	string ToString(QualifiedNameToStringMode mode = QualifiedNameToStringMode::DEFAULT) const;

	hash_t Hash() const;
	bool operator==(const QualifiedName &rhs) const;
	bool operator!=(const QualifiedName &rhs) const;

	void Serialize(Serializer &serializer) const;
	static QualifiedName Deserialize(Deserializer &deserializer);

private:
	//! The full path (catalog/schema/name). The name is always the last element; the catalog/schema components that
	//! are actually present precede it. For now at most [catalog, schema, name] (single schema level).
	vector<Identifier> path;
	//! Always-empty identifier, returned by the accessors when a catalog/schema/name component is absent
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
