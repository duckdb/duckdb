//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/tableref/table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/tableref.hpp"

namespace duckdb {
//! Represents a Table producing function
class TableFunction : public TableRef {
public:
	TableFunction() : TableRef(TableReferenceType::TABLE_FUNCTION) {
	}

	unique_ptr<ParsedExpression> function;

public:
	string ToString() const override {
		return function->ToString();
	}

	bool Equals(const TableRef *other_) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a BaseTableRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a BaseTableRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
