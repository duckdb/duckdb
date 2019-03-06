//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/tableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace duckdb {
class Deserializer;
class Serializer;
class SQLNodeVisitor;

//! Represents a generic expression that returns a table.
class TableRef {
public:
	TableRef(TableReferenceType type) : type(type) {
	}
	virtual ~TableRef() {
	}

	virtual unique_ptr<TableRef> Accept(SQLNodeVisitor *v) = 0;
	virtual bool Equals(const TableRef *other) const {
		return other && type == other->type && alias == other->alias;
	}

	virtual unique_ptr<TableRef> Copy() = 0;

	//! Serializes a TableRef to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a TableRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);

	//! Convert the object to a string
	virtual string ToString() const {
		return string();
	}
	void Print();

	TableReferenceType type;
	string alias;
};
} // namespace duckdb
