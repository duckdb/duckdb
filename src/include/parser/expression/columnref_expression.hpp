//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/columnref_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
//! Represents a reference to a column from either the FROM clause or from an
//! alias
class ColumnRefExpression : public AbstractExpression {
  public:
	//! STAR expression in SELECT clause
	ColumnRefExpression()
	    : AbstractExpression(ExpressionType::STAR), index((size_t)-1),
	      reference(nullptr) {}
	//! Only specify the column name, the table name will be derived later
	ColumnRefExpression(std::string column_name)
	    : AbstractExpression(ExpressionType::COLUMN_REF),
	      column_name(column_name), index((size_t)-1), reference(nullptr) {}

	//! Specify both the column and table name
	ColumnRefExpression(std::string column_name, std::string table_name)
	    : AbstractExpression(ExpressionType::COLUMN_REF),
	      column_name(column_name), table_name(table_name), index((size_t)-1),
	      reference(nullptr) {}

	ColumnRefExpression(TypeId type, size_t index)
	    : AbstractExpression(ExpressionType::COLUMN_REF, type),
	      column_name(""), table_name(""), index(index),
	      reference(nullptr) {}

	const std::string &GetColumnName() const { return column_name; }
	const std::string &GetTableName() const { return table_name; }

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

	virtual void ResolveType() override {
		AbstractExpression::ResolveType();
		if (return_type == TypeId::INVALID) {
			throw Exception("Type of ColumnRefExpression was not resolved!");
		}
	}

	virtual bool Equals(const AbstractExpression *other_) override {
		if (!AbstractExpression::Equals(other_)) {
			return false;
		}
		auto other = reinterpret_cast<const ColumnRefExpression*>(other_);
		if (!other) {
			return false;
		}
		return column_name == other->column_name && table_name == other->table_name;
	}

	//! Column index set by the binder, used to access data in the executor
	size_t index;
	//! A reference to the AbstractExpression this references, only used for
	//! alias references
	AbstractExpression *reference;
	//! Column name that is referenced
	std::string column_name;
	//! Table name of the column name that is referenced (optional)
	std::string table_name;
};
} // namespace duckdb
