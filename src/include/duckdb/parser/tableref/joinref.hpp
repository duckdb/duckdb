//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/joinref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Represents a JOIN between two expressions
class JoinRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::JOIN;

public:
	explicit JoinRef(JoinRefType ref_type = JoinRefType::REGULAR)
	    : TableRef(TableReferenceType::JOIN), type(JoinType::INNER), ref_type(ref_type) {
	}

	//! The left hand side of the join
	unique_ptr<TableRef> left;
	//! The right hand side of the join
	unique_ptr<TableRef> right;
	//! The join condition
	unique_ptr<ParsedExpression> condition;
	//! The join type
	JoinType type;
	//! Join condition type
	JoinRefType ref_type;
	//! The set of USING columns (if any)
	vector<string> using_columns;
	//! Duplicate eliminated columns (if any)
	vector<unique_ptr<ParsedExpression>> duplicate_eliminated_columns;
	//! If we have duplicate eliminated columns if the delim is flipped
	bool delim_flipped = false;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a JoinRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
