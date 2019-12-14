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

enum class PragmaType : uint8_t { NOTHING, ASSIGNMENT, CALL };

struct PragmaInfo : public ParseInfo {
	//! Name of the PRAGMA statement
	string name;
	//! Type of pragma statement
	PragmaType pragma_type;
	//! Parameter list (if any)
	vector<Value> parameters;
};

} // namespace duckdb
