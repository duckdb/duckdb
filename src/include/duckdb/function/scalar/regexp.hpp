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
#include "duckdb/function/built_in_functions.hpp"
#include "re2/stringpiece.h"

namespace duckdb {

namespace regexp_util {

bool TryParseConstantPattern(ClientContext &context, Expression &expr, string &constant_string);
void ParseRegexOptions(const string &options, duckdb_re2::RE2::Options &result, bool *global_replace = nullptr);
void ParseRegexOptions(ClientContext &context, Expression &expr, RE2::Options &target, bool *global_replace = nullptr);

inline duckdb_re2::StringPiece CreateStringPiece(const string_t &input) {
	return duckdb_re2::StringPiece(input.GetData(), input.GetSize());
}

inline string_t Extract(const string_t &input, Vector &result, const RE2 &re, const duckdb_re2::StringPiece &rewrite) {
	string extracted;
	RE2::Extract(input.GetString(), re, rewrite, &extracted);
	return StringVector::AddString(result, extracted.c_str(), extracted.size());
}

} // namespace regexp_util

struct RegexpExtractAll {
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result);
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);
	static unique_ptr<FunctionLocalState> InitLocalState(ExpressionState &state, const BoundFunctionExpression &expr,
	                                                     FunctionData *bind_data);
};

struct RegexpBaseBindData : public FunctionData {
	RegexpBaseBindData();
	RegexpBaseBindData(duckdb_re2::RE2::Options options, string constant_string, bool constant_pattern = true);
	~RegexpBaseBindData() override;

	duckdb_re2::RE2::Options options;
	string constant_string;
	bool constant_pattern;

	bool Equals(const FunctionData &other_p) const override;
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

struct RegexStringPieceArgs {
	RegexStringPieceArgs() : size(0), capacity(0), group_buffer(nullptr) {
	}
	void Init(idx_t size) {
		this->size = size;
		// Allocate for one extra, for the all-encompassing match group
		this->capacity = size + 1;
		group_buffer = AllocateArray<duckdb_re2::StringPiece>(capacity);
	}
	void SetSize(idx_t size) {
		this->size = size;
		if (size + 1 > capacity) {
			Clear();
			Init(size);
		}
	}

	RegexStringPieceArgs &operator=(RegexStringPieceArgs &&other) noexcept {
		std::swap(this->size, other.size);
		std::swap(this->capacity, other.capacity);
		std::swap(this->group_buffer, other.group_buffer);
		return *this;
	}

	~RegexStringPieceArgs() {
		Clear();
	}

private:
	void Clear() {
		DeleteArray<duckdb_re2::StringPiece>(group_buffer, capacity);
		group_buffer = nullptr;

		size = 0;
		capacity = 0;
	}

public:
	idx_t size;
	//! The currently allocated capacity for the groups
	idx_t capacity;
	//! Used by ExtractAll to pre-allocate the storage for the groups
	duckdb_re2::StringPiece *group_buffer;
};

struct RegexLocalState : public FunctionLocalState {
	explicit RegexLocalState(RegexpBaseBindData &info, bool extract_all = false)
	    : constant_pattern(duckdb_re2::StringPiece(info.constant_string.c_str(), info.constant_string.size()),
	                       info.options) {
		if (extract_all) {
			auto group_count_p = constant_pattern.NumberOfCapturingGroups();
			if (group_count_p != -1) {
				group_buffer.Init(group_count_p);
			}
		}
		D_ASSERT(info.constant_pattern);
	}

	RE2 constant_pattern;
	//! Used by regexp_extract_all to pre-allocate the args
	RegexStringPieceArgs group_buffer;
};

unique_ptr<FunctionLocalState> RegexInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                   FunctionData *bind_data);
unique_ptr<FunctionData> RegexpMatchesBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments);

} // namespace duckdb
