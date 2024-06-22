//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/parse_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/catalog_type.hpp"

namespace duckdb {

enum class CatalogType : uint8_t;

enum class ParseInfoType : uint8_t {
	ALTER_INFO,
	ATTACH_INFO,
	COPY_INFO,
	CREATE_INFO,
	CREATE_SECRET_INFO,
	DETACH_INFO,
	DROP_INFO,
	BOUND_EXPORT_DATA,
	LOAD_INFO,
	PRAGMA_INFO,
	SHOW_SELECT_INFO,
	TRANSACTION_INFO,
	VACUUM_INFO,
	COMMENT_ON_INFO,
	COMMENT_ON_COLUMN_INFO,
	COPY_DATABASE_INFO,
	UPDATE_EXTENSIONS_INFO
};

struct ParseInfo {
	explicit ParseInfo(ParseInfoType info_type) : info_type(info_type) {
	}
	virtual ~ParseInfo() {
	}

	ParseInfoType info_type;

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
	static string QualifierToString(const string &catalog, const string &schema, const string &name);
	static string TypeToString(CatalogType type);
};

} // namespace duckdb
