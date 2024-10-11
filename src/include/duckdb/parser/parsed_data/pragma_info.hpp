//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/pragma_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

enum class PragmaType : uint8_t { PRAGMA_STATEMENT, PRAGMA_CALL };

struct PragmaInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::PRAGMA_INFO;

public:
	PragmaInfo() : ParseInfo(TYPE) {
	}

	//! Name of the PRAGMA statement
	string name;
	//! Parameter list (if any)
	vector<unique_ptr<ParsedExpression>> parameters;
	//! Named parameter list (if any)
	case_insensitive_map_t<unique_ptr<ParsedExpression>> named_parameters;

public:
	unique_ptr<PragmaInfo> Copy() const;
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
