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
	RegexpMatchesBindData(duckdb_re2::RE2::Options options, string constant_string);
	~RegexpMatchesBindData() override;

	duckdb_re2::RE2::Options options;
	string constant_string;
	bool constant_pattern;
	string range_min;
	string range_max;
	bool range_success;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

struct RegexpReplaceBindData : public FunctionData {
	duckdb_re2::RE2::Options options;
	bool global_replace;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

struct RegexpExtractBindData : public FunctionData {
	RegexpExtractBindData(bool constant_pattern, const string &pattern, const string &group_string_p);

	const bool constant_pattern;
	const string constant_string;

	const string group_string;
	const duckdb_re2::StringPiece rewrite;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

} // namespace duckdb
