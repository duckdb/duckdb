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

struct PivotColumnEntry {
	//! The set of values to match on
	vector<Value> values;
	//! The star expression (UNPIVOT only)
	unique_ptr<ParsedExpression> star_expr;
	//! The alias of the pivot column entry
	string alias;

	bool Equals(const PivotColumnEntry &other) const;
	void Serialize(Serializer &serializer) const;
	PivotColumnEntry Copy() const;
	static PivotColumnEntry Deserialize(Deserializer &source);

	void FormatSerialize(FormatSerializer &serializer) const;
	static PivotColumnEntry FormatDeserialize(FormatDeserializer &source);
};

struct PivotColumn {
	//! The column names to (un)pivot
	vector<string> names;
	//! The set of values to pivot on
	vector<PivotColumnEntry> entries;
	//! The enum to read pivot values from (if any)
	string pivot_enum;

	string ToString() const;
	bool Equals(const PivotColumn &other) const;
	void Serialize(Serializer &serializer) const;
	PivotColumn Copy() const;
	static PivotColumn Deserialize(Deserializer &source);

	void FormatSerialize(FormatSerializer &serializer) const;
	static PivotColumn FormatDeserialize(FormatDeserializer &source);
};

//! Represents a PIVOT or UNPIVOT expression
class PivotRef : public TableRef {
public:
	explicit PivotRef() : TableRef(TableReferenceType::PIVOT), include_nulls(false) {
	}

	//! The source table of the pivot
	unique_ptr<TableRef> source;
	//! The aggregates to compute over the pivot (PIVOT only)
	vector<unique_ptr<ParsedExpression>> aggregates;
	//! The names of the unpivot expressions (UNPIVOT only)
	vector<string> unpivot_names;
	//! The set of pivots
	vector<PivotColumn> pivots;
	//! The groups to pivot over. If none are specified all columns not included in the pivots/aggregate are chosen.
	vector<string> groups;
	//! Aliases for the column names
	vector<string> column_name_alias;
	//! Whether or not to include nulls in the result (UNPIVOT only)
	bool include_nulls;

public:
	string ToString() const override;
	bool Equals(const TableRef *other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a JoinRef
	void Serialize(FieldWriter &serializer) const override;
	//! Deserializes a blob back into a JoinRef
	static unique_ptr<TableRef> Deserialize(FieldReader &source);

	void FormatSerialize(FormatSerializer &serializer) const override;
	static unique_ptr<TableRef> FormatDeserialize(FormatDeserializer &source);
};
} // namespace duckdb
