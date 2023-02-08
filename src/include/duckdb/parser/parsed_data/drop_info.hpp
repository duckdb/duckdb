//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/drop_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct DropInfo : public ParseInfo {
	DropInfo() : catalog(INVALID_CATALOG), schema(INVALID_SCHEMA), if_exists(false), cascade(false) {
	}

	//! The catalog type to drop
	CatalogType type;
	//! Catalog name to drop from, if any
	string catalog;
	//! Schema name to drop from, if any
	string schema;
	//! Element name to drop
	string name;
	//! Ignore if the entry does not exist instead of failing
	bool if_exists = false;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;
	//! Allow dropping of internal system entries
	bool allow_drop_internal = false;

public:
	unique_ptr<DropInfo> Copy() const {
		auto result = make_unique<DropInfo>();
		result->type = type;
		result->catalog = catalog;
		result->schema = schema;
		result->name = name;
		result->if_exists = if_exists;
		result->cascade = cascade;
		result->allow_drop_internal = allow_drop_internal;
		return result;
	}

	void Serialize(Serializer &serializer) const {
		FieldWriter writer(serializer);
		writer.WriteField<CatalogType>(type);
		writer.WriteString(catalog);
		writer.WriteString(schema);
		writer.WriteString(name);
		writer.WriteField(if_exists);
		writer.WriteField(cascade);
		writer.WriteField(allow_drop_internal);
		writer.Finalize();
	}

	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer) {
		FieldReader reader(deserializer);
		auto drop_info = make_unique<DropInfo>();
		drop_info->type = reader.ReadRequired<CatalogType>();
		drop_info->catalog = reader.ReadRequired<string>();
		drop_info->schema = reader.ReadRequired<string>();
		drop_info->name = reader.ReadRequired<string>();
		drop_info->if_exists = reader.ReadRequired<bool>();
		drop_info->cascade = reader.ReadRequired<bool>();
		drop_info->allow_drop_internal = reader.ReadRequired<bool>();
		reader.Finalize();
		return std::move(drop_info);
	}
};

} // namespace duckdb
