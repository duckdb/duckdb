//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_schema_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

struct CreateSchemaInfo : public CreateInfo {
	CreateSchemaInfo() : CreateInfo(CatalogType::SCHEMA_ENTRY) {
	}

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateSchemaInfo>();
		CopyProperties(*result);
		return std::move(result);
	}

	static unique_ptr<CreateSchemaInfo> Deserialize(Deserializer &deserializer) {
		auto result = make_unique<CreateSchemaInfo>();
		result->DeserializeBase(deserializer);
		return result;
	}

protected:
	void SerializeInternal(Serializer &) const override {
	}
};

} // namespace duckdb
