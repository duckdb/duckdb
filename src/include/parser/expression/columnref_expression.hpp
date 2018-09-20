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

struct ColumnBinding {
	size_t table_index;
	size_t column_index;

	ColumnBinding() : table_index((size_t)-1), column_index((size_t)-1) {}
	ColumnBinding(size_t table, size_t column)
	    : table_index(table), column_index(column) {}

	bool operator==(const ColumnBinding &rhs) {
		return table_index == rhs.table_index &&
		       column_index == rhs.column_index;
	}
};

//! Represents a reference to a column from either the FROM clause or from an
//! alias
class ColumnRefExpression : public AbstractExpression {
  public:
	//! STAR expression in SELECT clause
	ColumnRefExpression()
	    : AbstractExpression(ExpressionType::STAR), reference(nullptr) {}
	//! Only specify the column name, the table name will be derived later
	ColumnRefExpression(std::string column_name)
	    : AbstractExpression(ExpressionType::COLUMN_REF), reference(nullptr),
	      column_name(column_name) {}

	//! Specify both the column and table name
	ColumnRefExpression(std::string column_name, std::string table_name)
	    : AbstractExpression(ExpressionType::COLUMN_REF), reference(nullptr),
	      column_name(column_name), table_name(table_name) {}

	ColumnRefExpression(TypeId type, ColumnBinding binding)
	    : AbstractExpression(ExpressionType::COLUMN_REF, type),
	      binding(binding), reference(nullptr), column_name(""),
	      table_name("") {}

	ColumnRefExpression(TypeId type, size_t index)
	    : AbstractExpression(ExpressionType::COLUMN_REF, type), index(index),
	      reference(nullptr), column_name(""), table_name("") {}

	const std::string &GetColumnName() const { return column_name; }
	const std::string &GetTableName() const { return table_name; }

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

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
		auto other = reinterpret_cast<const ColumnRefExpression *>(other_);
		if (!other) {
			return false;
		}
		return column_name == other->column_name &&
		       table_name == other->table_name;
	}

	// FIXME: move these

	//! Column index set by the binder, used to access data in the executor
	ColumnBinding binding;

	//! Index used to access data in the chunks, set by the
	//! ColumnBindingResolver
	size_t index = (size_t)-1;

	//! Subquery recursion depth, needed for execution
	size_t depth = 0;

	//! A reference to the AbstractExpression this references, only used for
	//! alias references
	AbstractExpression *reference;
	//! Column name that is referenced
	std::string column_name;
	//! Table name of the column name that is referenced (optional)
	std::string table_name;

	virtual std::string ToString() const override {
		if (index != (size_t)-1) {
			return "#" + std::to_string(index);
		}
		auto str = table_name.empty() ? std::to_string(binding.table_index)
		                              : table_name;
		str += ".";
		str += column_name.empty() ? std::to_string(binding.column_index)
		                           : column_name;
		return str;
	}
};
} // namespace duckdb
