#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

struct RegularStringSplit {
	static idx_t Find(const char *input_data, idx_t input_size, const char *delim_data, idx_t delim_size,
	                  idx_t &match_size, void *data) {
		match_size = delim_size;
		if (delim_size == 0) {
			return 0;
		}
		return FindStrInStr(const_uchar_ptr_cast(input_data), input_size, const_uchar_ptr_cast(delim_data), delim_size);
	}
};

struct ConstantRegexpStringSplit {
	static idx_t Find(const char *input_data, idx_t input_size, const char *delim_data, idx_t delim_size,
	                  idx_t &match_size, void *data) {
		D_ASSERT(data);
		auto regex = reinterpret_cast<duckdb_re2::RE2 *>(data);
		duckdb_re2::StringPiece match;
		if (!regex->Match(duckdb_re2::StringPiece(input_data, input_size), 0, input_size, RE2::UNANCHORED, &match, 1)) {
			return DConstants::INVALID_INDEX;
		}
		match_size = match.size();
		return UnsafeNumericCast<idx_t>(match.data() - input_data);
	}
};

struct RegexpStringSplit {
	static idx_t Find(const char *input_data, idx_t input_size, const char *delim_data, idx_t delim_size,
	                  idx_t &match_size, void *data) {
		duckdb_re2::RE2 regex(duckdb_re2::StringPiece(delim_data, delim_size));
		if (!regex.ok()) {
			throw InvalidInputException(regex.error());
		}
		return ConstantRegexpStringSplit::Find(input_data, input_size, delim_data, delim_size, match_size, &regex);
	}
};

struct StringSplitter {
	template <class OP, class CB>
	static idx_t Split(string_t input, string_t delim, void *data, CB &&emit) {
		auto input_data = input.GetData();
		auto input_size = input.GetSize();
		auto delim_data = delim.GetData();
		auto delim_size = delim.GetSize();
		idx_t list_idx = 0;
		while (input_size > 0) {
			idx_t match_size = 0;
			auto pos = OP::Find(input_data, input_size, delim_data, delim_size, match_size, data);
			if (pos > input_size) {
				break;
			}
			if (match_size == 0 && pos == 0) {
				// special case: 0 length match and pos is 0
				// move to the next character
				for (pos++; pos < input_size; pos++) {
					if (IsCharacter(input_data[pos])) {
						break;
					}
				}
				if (pos == input_size) {
					break;
				}
			}
			D_ASSERT(input_size >= pos + match_size);
			emit(input_data, pos);
			list_idx++;
			input_data += (pos + match_size);
			input_size -= (pos + match_size);
		}
		emit(input_data, input_size);
		list_idx++;
		return list_idx;
	}
};

template <class OP>
void StringSplitExecutor(DataChunk &args, ExpressionState &state, Vector &result, void *data = nullptr) {
	auto input_entries = args.data[0].Values<string_t>(args.size());
	auto delim_entries = args.data[1].Values<string_t>(args.size());

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto list_writer = FlatVector::Writer<VectorListType<string_t>>(result, args.size());
	for (idx_t i = 0; i < args.size(); i++) {
		auto input_entry = input_entries[i];
		if (!input_entry.IsValid()) {
			list_writer.WriteNull();
			continue;
		}
		auto delim_entry = delim_entries[i];
		auto list = list_writer.WriteDynamicList();
		if (!delim_entry.IsValid()) {
			// delim is NULL: copy the complete entry
			list.WriteElement().WriteStringRef(input_entry.GetValue());
			continue;
		}
		StringSplitter::Split<OP>(
		    input_entry.GetValue(), delim_entry.GetValue(), data, [&](const char *split_data, idx_t split_size) {
			    list.WriteElement().WriteStringRef(string_t(split_data, UnsafeNumericCast<uint32_t>(split_size)));
		    });
	}

	StringVector::AddHeapReference(ListVector::GetChildMutable(result), args.data[0]);
}

void StringSplitFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	StringSplitExecutor<RegularStringSplit>(args, state, result, nullptr);
}

void StringSplitRegexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<RegexpMatchesBindData>();
	if (info.constant_pattern) {
		// fast path: pre-compiled regex
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RegexLocalState>();
		StringSplitExecutor<ConstantRegexpStringSplit>(args, state, result, &lstate.constant_pattern);
	} else {
		// slow path: have to re-compile regex for every row
		StringSplitExecutor<RegexpStringSplit>(args, state, result);
	}
}

} // namespace

ScalarFunction StringSplitFun::GetFunction() {
	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);

	ScalarFunction string_split({LogicalType::VARCHAR, LogicalType::VARCHAR}, varchar_list_type, StringSplitFunction);
	string_split.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return string_split;
}

ScalarFunctionSet StringSplitRegexFun::GetFunctions() {
	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);
	ScalarFunctionSet regexp_split;
	ScalarFunction regex_fun({LogicalType::VARCHAR, LogicalType::VARCHAR}, varchar_list_type, StringSplitRegexFunction,
	                         RegexpMatchesBind, nullptr, RegexInitLocalState, LogicalType::INVALID,
	                         FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING);
	regexp_split.AddFunction(regex_fun);
	// regexp options
	regex_fun.GetArguments().emplace_back(LogicalType::VARCHAR);
	regexp_split.AddFunction(regex_fun);
	return regexp_split;
}

} // namespace duckdb
