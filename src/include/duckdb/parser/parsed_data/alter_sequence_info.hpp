//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_sequence_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/parsed_data/alter_sequence_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// AlterSequenceInfo
//===--------------------------------------------------------------------===//
enum class AlterSequenceType : uint8_t { INVALID = 0, CHANGE_OWNERSHIP = 1 };

struct AlterSequenceInfo : public AlterInfo {
	AlterSequenceInfo(string schema, string name) : AlterInfo(AlterType::ALTER_SEQUENCE, schema, name) {
	}
	~AlterSequenceInfo() override {
	}

	AlterSequenceType alter_sequence_type;

public:
	CatalogType GetCatalogType() override {
		return CatalogType::SEQUENCE_ENTRY;
	}
	virtual unique_ptr<AlterInfo> Copy() const override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source);
};
//===--------------------------------------------------------------------===//
// ChangeOwnershipInfo
//===--------------------------------------------------------------------===//

struct ChangeOwnershipInfo : public AlterSequenceInfo {
	ChangeOwnershipInfo(string schema, string name, string table_schema, string table_name)
	    : AlterSequenceInfo(schema, name), table_schema(table_schema), table_name(table_name) {

		alter_sequence_type = AlterSequenceType::CHANGE_OWNERSHIP;
	}
	~ChangeOwnershipInfo() override {
	}

	//! Table schema
	string table_schema;

	//! Table name
	string table_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string name);
};
} // namespace duckdb