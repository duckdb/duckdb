//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/tableref/tableref.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "parser/sql_node_visitor.hpp"

namespace duckdb {
//! Represents a generic expression that returns a table.
class TableRef : public Printable {
  public:
	TableRef(TableReferenceType type) : type(type) {}

	virtual void Accept(SQLNodeVisitor *v) = 0;

	virtual std::unique_ptr<TableRef> Copy() = 0;

	//! Serializes a TableRef to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a TableRef
	static std::unique_ptr<TableRef> Deserialize(Deserializer &source);

	//! Convert the object to a string
	virtual std::string ToString() const { return std::string(); }

	TableReferenceType type;
	std::string alias;
};
} // namespace duckdb
