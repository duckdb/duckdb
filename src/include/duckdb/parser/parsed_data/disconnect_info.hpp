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

	//! Optional target — empty for bare `DISCONNECT;` and for `DISCONNECT LOCAL;` (in the latter
	//! case, `target_is_local` is true). Otherwise either an identifier or the contents of a
	//! string literal.
	string name;
	//! True iff the target was parsed as the LOCAL keyword.
	bool target_is_local = false;
	//! True iff the target was parsed as a StringLiteral.
	bool name_is_string_literal = false;

public:
	unique_ptr<DisconnectInfo> Copy() const;
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
