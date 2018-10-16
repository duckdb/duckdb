//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/tableref_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/printable.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {
//! Represents a generic expression that returns a table.
class TableRef : public Printable {
  public:
	TableRef(TableReferenceType ref_type) : ref_type(ref_type) {}

	virtual void Accept(SQLNodeVisitor *v) = 0;

	virtual std::unique_ptr<TableRef> Copy() = 0;

	//! Convert the object to a string
	virtual std::string ToString() const { return std::string(); }

	TableReferenceType ref_type;
	std::string alias;
};
} // namespace duckdb
