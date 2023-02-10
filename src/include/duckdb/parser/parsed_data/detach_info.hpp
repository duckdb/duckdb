//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/detach_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

struct DetachInfo : public ParseInfo {
	DetachInfo() {
	}

	//! The alias of the attached database
	string name;
	//! Whether to throw an exception if alias is not found
	bool error_if_not_exist;

public:
	unique_ptr<DetachInfo> Copy() const {
		auto result = make_unique<DetachInfo>();
		result->name = name;
		result->error_if_not_exist = error_if_not_exist;
		return result;
	}
};
} // namespace duckdb
