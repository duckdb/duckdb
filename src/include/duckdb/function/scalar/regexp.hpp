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

struct RegexpBaseBindData : public FunctionData {
	RegexpBaseBindData();
	RegexpBaseBindData(duckdb_re2::RE2::Options options, string constant_string, bool constant_pattern = true);
	virtual ~RegexpBaseBindData();

	duckdb_re2::RE2::Options options;
	string constant_string;
	bool constant_pattern;

	virtual bool Equals(const FunctionData &other_p) const override;
};

struct RegexpMatchesBindData : public RegexpBaseBindData {
	RegexpMatchesBindData(duckdb_re2::RE2::Options options, string constant_string, bool constant_pattern);
	RegexpMatchesBindData(duckdb_re2::RE2::Options options, string constant_string, bool constant_pattern,
	                      string range_min, string range_max, bool range_success);

	string range_min;
	string range_max;
	bool range_success;

	unique_ptr<FunctionData> Copy() const override;
};

struct RegexpReplaceBindData : public RegexpBaseBindData {
	RegexpReplaceBindData();
	RegexpReplaceBindData(duckdb_re2::RE2::Options options, string constant_string, bool constant_pattern,
	                      bool global_replace);

	bool global_replace;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

struct RegexpExtractBindData : public RegexpBaseBindData {
	RegexpExtractBindData();
	RegexpExtractBindData(duckdb_re2::RE2::Options options, string constant_string, bool constant_pattern,
	                      string group_string);

	string group_string;
	duckdb_re2::StringPiece rewrite;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

} // namespace duckdb
