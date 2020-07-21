//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/regexp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "re2/re2.h"

namespace duckdb {

struct RegexpMatchesBindData : public FunctionData {
	RegexpMatchesBindData(re2::RE2::Options options, std::unique_ptr<re2::RE2> constant_pattern, string range_min, string range_max,
	                      bool range_success);
	~RegexpMatchesBindData();

	re2::RE2::Options options;
	std::unique_ptr<re2::RE2> constant_pattern;
	string range_min, range_max;
	bool range_success;

	unique_ptr<FunctionData> Copy() override;
};

struct RegexpReplaceBindData : public FunctionData {
	re2::RE2::Options options;
	bool global_replace;

	unique_ptr<FunctionData> Copy() override;
};

}
