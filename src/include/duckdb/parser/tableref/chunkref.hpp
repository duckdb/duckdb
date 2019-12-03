//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/tableref/chunkref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"

namespace duckdb {
//! Represents a scan of a DataChunk
class ChunkRef : public TableRef {
public:
	ChunkRef() : TableRef(TableReferenceType::CHUNK_DATA) {
	}

	//! The chunk to scan
	DataChunk chunk;
public:
	bool Equals(const TableRef *other_) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a CrossProductRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a CrossProductRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
