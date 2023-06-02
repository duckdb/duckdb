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
	DropInfo();

	//! The catalog type to drop
	CatalogType type;
	//! Catalog name to drop from, if any
	string catalog;
	//! Schema name to drop from, if any
	string schema;
	//! Element name to drop
	string name;
	//! Ignore if the entry does not exist instead of failing
	OnEntryNotFound if_not_found = OnEntryNotFound::THROW_EXCEPTION;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;
	//! Allow dropping of internal system entries
	bool allow_drop_internal = false;

public:
	unique_ptr<DropInfo> Copy() const;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
