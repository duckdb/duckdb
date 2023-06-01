//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/detach_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct DetachInfo : public ParseInfo {
	DetachInfo() {
	}

	//! The alias of the attached database
	string name;
	//! Whether to throw an exception if alias is not found
	bool if_exists;

public:
	unique_ptr<DetachInfo> Copy() const {
		auto result = make_uniq<DetachInfo>();
		result->name = name;
		result->if_exists = if_exists;
		return result;
	}
};
} // namespace duckdb
