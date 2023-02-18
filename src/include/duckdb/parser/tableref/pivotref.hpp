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
	//! The set of aliases for the value (if any)
	vector<string> aliases;
	//! The enum to read pivot values from (if any)
	string pivot_enum;

	string ToString() const;
};

//! Represents a PIVOT or UNPIVOT expression
class PivotRef : public TableRef {
public:
	explicit PivotRef() : TableRef(TableReferenceType::PIVOT) {
	}

	//! The source table of the pivot
	unique_ptr<TableRef> source;
	//! The aggregate to compute over the pivot (PIVOT only)
	unique_ptr<ParsedExpression> aggregate;
	//! The name of the unpivot expression (UNPIVOT only)
	string unpivot_name;
	//! The set of pivots
	vector<PivotColumn> pivots;
	//! The groups to pivot over. If none are specified all columns not included in the pivots/aggregate are chosen.
	vector<string> groups;
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
