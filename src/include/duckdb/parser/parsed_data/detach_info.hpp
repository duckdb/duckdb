//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/detach_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"

namespace duckdb {

struct DetachInfo : public ParseInfo {
	DetachInfo() {
	}

	//! The alias of the attached database
	string name;
	//! Whether to throw an exception if alias is not found
	OnEntryNotFound if_not_found;

public:
	unique_ptr<DetachInfo> Copy() const {
		auto result = make_uniq<DetachInfo>();
		result->name = name;
		result->if_not_found = if_not_found;
		return result;
	}
};
} // namespace duckdb
