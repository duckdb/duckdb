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

namespace duckdb {

enum class PragmaType : uint8_t {
	PRAGMA_STATEMENT,
	PRAGMA_ASSIGNMENT,
	PRAGMA_CALL
};

struct PragmaInfo : public ParseInfo {
	//! Name of the PRAGMA statement
	string name;
	//! Type of pragma statement
	PragmaType pragma_type;
	//! Parameter list (if any)
	vector<Value> parameters;
};

} // namespace duckdb
