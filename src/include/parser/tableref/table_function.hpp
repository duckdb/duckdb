//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/tableref/table_function.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/tableref.hpp"

namespace duckdb {
//! Represents a Table producing function
class TableFunction : public TableRef {
  public:
	TableFunction() : TableRef(TableReferenceType::TABLE_FUNCTION) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual bool Equals(const TableRef *other_) override {
		if (!TableRef::Equals(other_)) {
			return false;
		}
		auto other = (TableFunction *)other_;
		return function->Equals(other->function.get());
	}

	virtual std::unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a BaseTableRef
	virtual void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a BaseTableRef
	static std::unique_ptr<TableRef> Deserialize(Deserializer &source);

	virtual std::string ToString() const override {
		return function->ToString();
	}

	std::unique_ptr<Expression> function;
};
} // namespace duckdb
