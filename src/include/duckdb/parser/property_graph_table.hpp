//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckdb/parser/property_graph_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Represents a reference to a graph table from the CREATE PROPERTY GRAPH
class PropertyGraphTable {
public:
	//! Used for Copy
	PropertyGraphTable();
	//! Specify both the column and table name
	PropertyGraphTable(string table_name, vector<string> column_name,
		vector<string> label, string catalog_name = "",
		string schema = DEFAULT_SCHEMA);
	//! Specify both the column and table name with alias
	PropertyGraphTable(string table_name, string table_alias,
		vector<string> column_name, vector<string> label,
		string catalog_name = "", string schema = DEFAULT_SCHEMA);
	string catalog_name;
	string schema_name;
	string table_name;
	string table_name_alias;

	//! The stack of names in order of which they appear (column_names[0], column_names[1], column_names[2], ...)
	vector<string> column_names;
	vector<string> column_aliases;

	vector<string> except_columns;

	vector<string> sub_labels;
	string main_label;

	//! Associated with the PROPERTIES keyword not mentioned in the creation of table, equalling SELECT * in some sense
	bool all_columns = false;

	//! Associated with the NO PROPERTIES functionality
	bool no_columns = false;

	bool is_vertex_table = false;

	string discriminator;

	vector<string> source_fk;

	vector<string> source_pk;

	string source_reference;

	vector<string> destination_fk;

	vector<string> destination_pk;

	string destination_reference;

public:
	string ToString() const;
	bool Equals(const PropertyGraphTable *other_p) const;

	shared_ptr<PropertyGraphTable> Copy() const;

	void Serialize(Serializer &serializer) const;

	static shared_ptr<PropertyGraphTable> Deserialize(Deserializer &deserializer);

	bool hasTableNameAlias() {
		return !table_name_alias.empty();
	}

	string ToLower(const std::string &str);

	bool IsSourceTable(const string &table_name);
};
} // namespace duckdb
