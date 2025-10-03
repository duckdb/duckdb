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
	string catalog;
	string schema;
	string name;

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
