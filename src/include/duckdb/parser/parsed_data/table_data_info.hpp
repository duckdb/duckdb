//===--------------------------------------------------------------
//
// duckdb/parser/parsed_data/table_data_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

struct TableDataInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::TABLE_DATA_INFO;

	TableDataInfo() : ParseInfo(TYPE) {
	}

	TableDataType table_data_type;

	//! Schema of the table
	string schema;
	//! Name of the table
	string name;

public:
	unique_ptr<TableDataInfo> Copy();
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

//===--------------------------------------------------------------------===//
// Table Data Info
//===--------------------------------------------------------------------===//
enum class TableDataType : uint8_t {
	INVALID = 0,
	TABLE_DATA_INSERT_INFO = 1,
	TABLE_DATA_DELETE_INFO = 2,
	TABLE_DATA_UPDATE_INFO = 3,
	TABLE_DATA_ROW_GROUP_INFO = 4
};

struct TableDataInsertInfo : public TableDataInfo {
public:
	TableDataInsertInfo() : TableDataInfo() {
	}

	//! The chunk to insert
	unique_ptr<DataChunk> chunk;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableDataInfo> Deserialize(Deserializer &deserializer);
};

struct TableDataDeleteInfo : public TableDataInfo {
public:
	TableDataDeleteInfo() : TableDataInfo() {
	}

	//! The row IDs to delete
	unique_ptr<DataChunk> chunk;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableDataInfo> Deserialize(Deserializer &deserializer);
};

struct TableDataUpdateInfo : public TableDataInfo {
public:
	TableDataUpdateInfo() : TableDataInfo() {
	}

	//! The column indexes to update
	vector<column_t> column_indexes;
	//! The update chunk (including row IDs at the end)
	unique_ptr<DataChunk> chunk;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableDataInfo> Deserialize(Deserializer &deserializer);
};

struct TableDataRowGroupInfo : public TableDataInfo {
public:
	TableDataRowGroupInfo() : TableDataInfo() {
	}

	//! The persistent collection data containing row groups
	PersistentCollectionData collection_data;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableDataInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
