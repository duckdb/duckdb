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
	DUCKDB_API ForeignKeyConstraint(uint64_t fk_index, string pk_table, uint64_t pk_index);
	DUCKDB_API ForeignKeyConstraint(vector<string> fk_columns, string pk_table, vector<string> pk_columns);

	//! The referenced table's name
	string pk_table;
	//! The index of foreign key table's column.
	//! Only used when the constraint relates to a single column, equal to DConstants::INVALID_INDEX if not used
	uint64_t fk_index;
	//! The index of primary key table's column.
	//! Only used when the constraint relates to a single column, equal to DConstants::INVALID_INDEX if not used
	uint64_t pk_index;
	//! The set of columns for which this constraint holds by name. Only used when the index field is not used.
	vector<string> fk_columns;
	//! The set of referenced table's columns for which this constraint holds by name. Only used when the index field is
	//! not used.
	vector<string> pk_columns;

public:
	DUCKDB_API string ToString() const override;

	DUCKDB_API unique_ptr<Constraint> Copy() const override;

	//! Serialize to a stand-alone binary blob
	DUCKDB_API void Serialize(FieldWriter &writer) const override;
	//! Deserializes a ParsedConstraint
	DUCKDB_API static unique_ptr<Constraint> Deserialize(FieldReader &source);
};

} // namespace duckdb
