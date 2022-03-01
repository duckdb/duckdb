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
	//! Name of the PRAGMA statement
	string name;
	//! Parameter list (if any)
	vector<Value> parameters;
	//! Named parameter list (if any)
	named_parameter_map_t named_parameters;

public:
	unique_ptr<PragmaInfo> Copy() const {
		auto result = make_unique<PragmaInfo>();
		result->name = name;
		result->parameters = parameters;
		result->named_parameters = named_parameters;
		return result;
	}
};

} // namespace duckdb
