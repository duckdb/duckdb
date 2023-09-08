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
		auto result = make_uniq<CreateSchemaInfo>();
		CopyProperties(*result);
		return std::move(result);
	}

	DUCKDB_API void FormatSerialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> FormatDeserialize(Deserializer &deserializer);
};

} // namespace duckdb
