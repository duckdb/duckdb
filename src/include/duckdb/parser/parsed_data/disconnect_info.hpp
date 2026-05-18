//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/disconnect_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct DisconnectInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::DISCONNECT_INFO;

public:
	DisconnectInfo() : ParseInfo(TYPE) {
	}

public:
	unique_ptr<DisconnectInfo> Copy() const;
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
