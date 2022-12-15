//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/attach_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct AttachInfo : public ParseInfo {
	AttachInfo() {
	}

	//! The alias of the attached database
	string name;
	//! The path to the attached database
	string path;
	//! Set of (key, value) options
	unordered_map<string, vector<Value>> options;

public:
	unique_ptr<AttachInfo> Copy() const {
		auto result = make_unique<AttachInfo>();
		result->name = name;
		result->path = path;
		result->options = options;
		return result;
	}
};

} // namespace duckdb
