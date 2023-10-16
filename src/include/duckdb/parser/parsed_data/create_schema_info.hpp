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

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ToString() const override {
		string ret = "";
		switch (on_conflict) {
		case OnCreateConflict::ALTER_ON_CONFLICT: {
			ret += "CREATE SCHEMA " + schema + " ON CONFLICT INSERT OR REPLACE;";
			break;
		}
		case OnCreateConflict::IGNORE_ON_CONFLICT: {
			ret += "CREATE SCHEMA " + schema + " IF NOT EXISTS;";
			break;
		}
		case OnCreateConflict::REPLACE_ON_CONFLICT: {
			ret += "CREATE OR REPLACE SCHEMA " + schema + ";";
			break;
		}
		case OnCreateConflict::ERROR_ON_CONFLICT: {
			ret += "CREATE SCHEMA " + schema + ";";
			break;
		}
		}
		return ret;
	}
};

} // namespace duckdb
