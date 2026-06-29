//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/connect_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct ConnectInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::CONNECT_INFO;

public:
	ConnectInfo() : ParseInfo(TYPE) {
	}

	//! Target — empty for bare `CONNECT;` and for `CONNECT LOCAL;` (in the latter case,
	//! `target_is_local` is true). Otherwise either an identifier (attached-db name) or the
	//! contents of a string literal (connection-string form).
	Identifier name;
	//! True iff the target was parsed as the LOCAL keyword (`CONNECT LOCAL;`). When true, `name`
	//! is empty and `name_is_string_literal` is false.
	bool target_is_local = false;
	//! True iff the target was parsed as a StringLiteral. Differentiates `CONNECT 'foo'`
	//! (connection-string form) from `CONNECT foo` (attached-db identifier) at the AST level,
	//! so ToString roundtrips the source form and downstream impl can dispatch correctly.
	bool name_is_string_literal = false;

public:
	unique_ptr<ConnectInfo> Copy() const;
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
