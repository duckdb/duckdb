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

#include "common/common.hpp"
#include "common/printable.hpp"

namespace duckdb {
class SQLNodeVisitor;
//! Represents a generic expression that returns a table.
class TableRef : public Printable {
  public:
	TableRef(TableReferenceType type) : type(type) {
	}

	virtual std::unique_ptr<TableRef> Accept(SQLNodeVisitor *v) = 0;
	virtual bool Equals(const TableRef *other) {
		return other && type == other->type && alias == other->alias;
	}

	virtual std::unique_ptr<TableRef> Copy() = 0;

	//! Serializes a TableRef to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a TableRef
	static std::unique_ptr<TableRef> Deserialize(Deserializer &source);

	//! Convert the object to a string
	virtual std::string ToString() const {
		return std::string();
	}

	TableReferenceType type;
	std::string alias;
};
} // namespace duckdb
