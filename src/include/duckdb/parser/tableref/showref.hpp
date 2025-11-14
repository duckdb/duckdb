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
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

enum class ShowType : uint8_t { SUMMARY, DESCRIBE, SHOW_FROM };

//! Represents a SHOW/DESCRIBE/SUMMARIZE statement
class ShowRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::SHOW_REF;

public:
	ShowRef();

	//! The table name (if any)
	string table_name;
	//! The catalog name (if any)
	string catalog_name;
	//! The schema name (if any)
	string schema_name;
	//! The QueryNode of select query (if any)
	unique_ptr<QueryNode> query;
	//! Whether or not we are requesting a summary or a describe
	ShowType show_type;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a ExpressionListRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
