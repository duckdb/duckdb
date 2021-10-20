//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/vacuum_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct LoadInfo : public ParseInfo {
	std::string filename;
	bool install;

public:
	unique_ptr<LoadInfo> Copy() const {
		auto result = make_unique<LoadInfo>();
		result->filename = filename;
		result->install = install;
		return result;
	}
};

} // namespace duckdb
