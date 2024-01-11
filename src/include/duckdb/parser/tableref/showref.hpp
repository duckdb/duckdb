//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/showref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

//! Represents a SHOW/DESCRIBE/SUMMARIZE statement
class ShowRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::SHOW_REF;

public:
	ShowRef();

	//! The QueryNode of select query
	unique_ptr<QueryNode> query;
	//! Whether or not we are requesting a summary or a describe
	bool is_summary;


public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a ExpressionListRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
