//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraints/foreign_key_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/constraint.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ForeignKeyConstraint : public Constraint {
public:
	DUCKDB_API ForeignKeyConstraint(string pk_table, vector<string> pk_columns, vector<idx_t> pk_keys,
	                                vector<string> fk_columns, vector<idx_t> fk_keys, bool is_fk_table);

	//! The referenced table's name
	string pk_table;
	//! The set of main key table's columns
	vector<string> pk_columns;
	//! The set of main key table's column's index
	vector<idx_t> pk_keys;
	//! The set of foreign key table's columns
	vector<string> fk_columns;
	//! The set of foreign key table's column's index
	vector<idx_t> fk_keys;
	//! if this is true, this table has foreign keys.
	//! if this is false, this table is referenced table.
	bool is_fk_table;

public:
	DUCKDB_API string ToString() const override;

	DUCKDB_API unique_ptr<Constraint> Copy() const override;

	//! Serialize to a stand-alone binary blob
	DUCKDB_API void Serialize(FieldWriter &writer) const override;
	//! Deserializes a ParsedConstraint
	DUCKDB_API static unique_ptr<Constraint> Deserialize(FieldReader &source);
};

} // namespace duckdb
