//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_database_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

struct CreateDatabaseInfo : public CreateInfo {
	CreateDatabaseInfo() : CreateInfo(CatalogType::DATABASE_ENTRY) {
	}

	//! Name of the database
	string name;

	//! Source path of the database if it's created from another database
	string path;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_uniq<CreateDatabaseInfo>();
		CopyProperties(*result);
		result->name = name;
		result->path = path;
		return unique_ptr<CreateInfo>(result.release());
	}

	static unique_ptr<CreateDatabaseInfo> Deserialize(Deserializer &deserializer) {
		auto result = make_uniq<CreateDatabaseInfo>();
		result->DeserializeBase(deserializer);
		return result;
	}

protected:
	void SerializeInternal(Serializer &) const override {
		throw NotImplementedException("Cannot serialize '%s'", CatalogTypeToString(type));
	}
};

} // namespace duckdb
