//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/refresh_matview_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct RefreshMatViewInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::REFRESH_MATVIEW_INFO;

public:
	RefreshMatViewInfo() : ParseInfo(TYPE) {
	}
	//! The name of the materialized view to refresh
	string name;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
