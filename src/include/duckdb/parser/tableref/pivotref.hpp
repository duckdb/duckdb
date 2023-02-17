//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/pivotref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"

namespace duckdb {

struct PivotColumn {
	//! The name of the pivot column
	string name;
	//! The set of values to pivot on
	vector<Value> values;

	string ToString() const;
};

//! Represents a JOIN between two expressions
class PivotRef : public TableRef {
public:
	explicit PivotRef() : TableRef(TableReferenceType::PIVOT) {
	}

	//! The source table of the pivot
	unique_ptr<TableRef> source;
	//! The set of aggregates to pivot over
	vector<unique_ptr<ParsedExpression>> aggregates;
	//! The set of pivots
	vector<PivotColumn> pivots;
	//! Aliases for the column names
	vector<string> column_name_alias;

public:
	string ToString() const override;
	bool Equals(const TableRef *other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a JoinRef
	void Serialize(FieldWriter &serializer) const override;
	//! Deserializes a blob back into a JoinRef
	static unique_ptr<TableRef> Deserialize(FieldReader &source);
};
} // namespace duckdb
