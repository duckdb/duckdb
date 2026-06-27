//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/showref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

enum class ShowType : uint8_t { SUMMARY, DESCRIBE, SHOW_FROM, SHOW_UNQUALIFIED };

//! Represents a SHOW/DESCRIBE/SUMMARIZE statement
class ShowRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::SHOW_REF;

public:
	ShowRef();

	//! The (optional) catalog/schema-qualified table name to show
	QualifiedName qualified_name;
	//! The QueryNode of select query (if any)
	unique_ptr<QueryNode> query;
	//! Whether or not we are requesting a summary or a describe
	ShowType show_type;

public:
	void SetQualifiedName(Identifier catalog, Identifier schema, Identifier name) {
		qualified_name = QualifiedName(std::move(catalog), std::move(schema), std::move(name));
	}
	//! The table name (if any)
	const Identifier &GetTableName() const {
		return qualified_name.Name();
	}
	void SetTableName(Identifier table_name) {
		qualified_name = qualified_name.WithName(std::move(table_name));
	}
	//! The catalog name (if any)
	const Identifier &GetCatalogName() const {
		return qualified_name.Catalog();
	}
	void SetCatalogName(Identifier catalog_name) {
		qualified_name = QualifiedName(std::move(catalog_name), qualified_name.Schema(), qualified_name.Name());
	}
	//! The schema name (if any)
	const Identifier &GetSchemaName() const {
		return qualified_name.Schema();
	}
	void SetSchemaName(Identifier schema_name) {
		qualified_name = QualifiedName(qualified_name.Catalog(), std::move(schema_name), qualified_name.Name());
	}

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a ExpressionListRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
