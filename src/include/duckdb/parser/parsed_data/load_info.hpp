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

enum class LoadType { LOAD, INSTALL, FORCE_INSTALL };

struct LoadInfo : public ParseInfo {
	std::string filename;
	LoadType load_type;

public:
	unique_ptr<LoadInfo> Copy() const {
		auto result = make_unique<LoadInfo>();
		result->filename = filename;
		result->load_type = load_type;
		return result;
	}
};

} // namespace duckdb
