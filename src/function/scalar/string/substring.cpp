#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "utf8proc.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

static const int64_t SUPPORTED_UPPER_BOUND = NumericLimits<uint32_t>::Maximum();
static const int64_t SUPPORTED_LOWER_BOUND = -SUPPORTED_UPPER_BOUND - 1;

static inline void AssertInSupportedRange(idx_t input_size, int64_t offset, int64_t length) {

	if (input_size > (uint64_t)SUPPORTED_UPPER_BOUND) {
		throw OutOfRangeException("Substring input size is too large (> %d)", SUPPORTED_UPPER_BOUND);
	}
	if (offset < SUPPORTED_LOWER_BOUND) {
		throw OutOfRangeException("Substring offset outside of supported range (< %d)", SUPPORTED_LOWER_BOUND);
	}
	if (offset > SUPPORTED_UPPER_BOUND) {
		throw OutOfRangeException("Substring offset outside of supported range (> %d)", SUPPORTED_UPPER_BOUND);
	}
	if (length < SUPPORTED_LOWER_BOUND) {
		throw OutOfRangeException("Substring length outside of supported range (< %d)", SUPPORTED_LOWER_BOUND);
	}
	if (length > SUPPORTED_UPPER_BOUND) {
		throw OutOfRangeException("Substring length outside of supported range (> %d)", SUPPORTED_UPPER_BOUND);
	}
}

string_t SubstringEmptyString(Vector &result) {
	auto result_string = StringVector::EmptyString(result, 0);
	result_string.Finalize();
	return result_string;
}

string_t SubstringSlice(Vector &result, const char *input_data, int64_t offset, int64_t length) {
	auto result_string = StringVector::EmptyString(result, UnsafeNumericCast<idx_t>(length));
	auto result_data = result_string.GetDataWriteable();
	memcpy(result_data, input_data + offset, UnsafeNumericCast<size_t>(length));
	result_string.Finalize();
	return result_string;
}

// compute start and end characters from the given input size and offset/length
bool SubstringStartEnd(int64_t input_size, int64_t offset, int64_t length, int64_t &start, int64_t &end) {
	if (length == 0) {
		return false;
	}
	if (offset > 0) {
		// positive offset: scan from start
		start = MinValue<int64_t>(input_size, offset - 1);
	} else if (offset < 0) {
		// negative offset: scan from end (i.e. start = end + offset)
		start = MaxValue<int64_t>(input_size + offset, 0);
	} else {
		// offset = 0: special case, we start 1 character BEHIND the first character
		start = 0;
		length--;
		if (length <= 0) {
			return false;
		}
	}
	if (length > 0) {
		// positive length: go forward (i.e. end = start + offset)
		end = MinValue<int64_t>(input_size, start + length);
	} else {
		// negative length: go backwards (i.e. end = start, start = start + length)
		end = start;
		start = MaxValue<int64_t>(0, start + length);
	}
	if (start == end) {
		return false;
	}
	D_ASSERT(start < end);
	return true;
}

string_t SubstringASCII(Vector &result, string_t input, int64_t offset, int64_t length) {
	auto input_data = input.GetData();
	auto input_size = input.GetSize();

	AssertInSupportedRange(input_size, offset, length);

	int64_t start, end;
	if (!SubstringStartEnd(UnsafeNumericCast<int64_t>(input_size), offset, length, start, end)) {
		return SubstringEmptyString(result);
	}
	return SubstringSlice(result, input_data, start, UnsafeNumericCast<int64_t>(end - start));
}

string_t SubstringFun::SubstringUnicode(Vector &result, string_t input, int64_t offset, int64_t length) {
	auto input_data = input.GetData();
	auto input_size = input.GetSize();

	AssertInSupportedRange(input_size, offset, length);

	if (length == 0) {
		return SubstringEmptyString(result);
	}
	// first figure out which direction we need to scan
	idx_t start_pos;
	idx_t end_pos;
	if (offset < 0) {
		start_pos = 0;
		end_pos = DConstants::INVALID_INDEX;

		// negative offset: scan backwards
		int64_t start, end;

		// we express start and end as unicode codepoints from the back
		offset--;
		if (length < 0) {
			// negative length
			start = -offset - length;
			end = -offset;
		} else {
			// positive length
			start = -offset;
			end = -offset - length;
		}
		if (end <= 0) {
			end_pos = input_size;
		}
		int64_t current_character = 0;
		for (idx_t i = input_size; i > 0; i--) {
			if (LengthFun::IsCharacter(input_data[i - 1])) {
				current_character++;
				if (current_character == start) {
					start_pos = i;
					break;
				} else if (current_character == end) {
					end_pos = i;
				}
			}
		}
		while (!LengthFun::IsCharacter(input_data[start_pos])) {
			start_pos++;
		}
		while (end_pos < input_size && !LengthFun::IsCharacter(input_data[end_pos])) {
			end_pos++;
		}

		if (end_pos == DConstants::INVALID_INDEX) {
			return SubstringEmptyString(result);
		}
	} else {
		start_pos = DConstants::INVALID_INDEX;
		end_pos = input_size;

		// positive offset: scan forwards
		int64_t start, end;

		// we express start and end as unicode codepoints from the front
		offset--;
		if (length < 0) {
			// negative length
			start = MaxValue<int64_t>(0, offset + length);
			end = offset;
		} else {
			// positive length
			start = MaxValue<int64_t>(0, offset);
			end = offset + length;
		}

		int64_t current_character = 0;
		for (idx_t i = 0; i < input_size; i++) {
			if (LengthFun::IsCharacter(input_data[i])) {
				if (current_character == start) {
					start_pos = i;
				} else if (current_character == end) {
					end_pos = i;
					break;
				}
				current_character++;
			}
		}
		if (start_pos == DConstants::INVALID_INDEX || end == 0 || end <= start) {
			return SubstringEmptyString(result);
		}
	}
	D_ASSERT(end_pos >= start_pos);
	// after we have found these, we can slice the substring
	return SubstringSlice(result, input_data, UnsafeNumericCast<int64_t>(start_pos),
	                      UnsafeNumericCast<int64_t>(end_pos - start_pos));
}

string_t SubstringFun::SubstringGrapheme(Vector &result, string_t input, int64_t offset, int64_t length) {
	auto input_data = input.GetData();
	auto input_size = input.GetSize();

	AssertInSupportedRange(input_size, offset, length);

	// we don't know yet if the substring is ascii, but we assume it is (for now)
	// first get the start and end as if this was an ascii string
	int64_t start, end;
	if (!SubstringStartEnd(UnsafeNumericCast<int64_t>(input_size), offset, length, start, end)) {
		return SubstringEmptyString(result);
	}

	// now check if all the characters between 0 and end are ascii characters
	// note that we scan one further to check for a potential combining diacritics (e.g. i + diacritic is ï)
	bool is_ascii = true;
	idx_t ascii_end = MinValue<idx_t>(UnsafeNumericCast<idx_t>(end + 1), input_size);
	for (idx_t i = 0; i < ascii_end; i++) {
		if (input_data[i] & 0x80) {
			// found a non-ascii character: eek
			is_ascii = false;
			break;
		}
	}
	if (is_ascii) {
		// all characters are ascii, we can just slice the substring
		return SubstringSlice(result, input_data, start, end - start);
	}
	// if the characters are not ascii, we need to scan grapheme clusters
	// first figure out which direction we need to scan
	// offset = 0 case is taken care of in SubstringStartEnd
	if (offset < 0) {
		// negative offset, this case is more difficult
		// we first need to count the number of characters in the string
		idx_t num_characters = Utf8Proc::GraphemeCount(input_data, input_size);
		// now call substring start and end again, but with the number of unicode characters this time
		SubstringStartEnd(UnsafeNumericCast<int64_t>(num_characters), offset, length, start, end);
	}

	// now scan the graphemes of the string to find the positions of the start and end characters
	int64_t current_character = 0;
	idx_t start_pos = DConstants::INVALID_INDEX, end_pos = input_size;
	for (auto cluster : Utf8Proc::GraphemeClusters(input_data, input_size)) {
		if (current_character == start) {
			start_pos = cluster.start;
		} else if (current_character == end) {
			end_pos = cluster.start;
			break;
		}
		current_character++;
	}
	if (start_pos == DConstants::INVALID_INDEX) {
		return SubstringEmptyString(result);
	}
	// after we have found these, we can slice the substring
	return SubstringSlice(result, input_data, UnsafeNumericCast<int64_t>(start_pos),
	                      UnsafeNumericCast<int64_t>(end_pos - start_pos));
}

struct SubstringUnicodeOp {
	static string_t Substring(Vector &result, string_t input, int64_t offset, int64_t length) {
		return SubstringFun::SubstringUnicode(result, input, offset, length);
	}
};

struct SubstringGraphemeOp {
	static string_t Substring(Vector &result, string_t input, int64_t offset, int64_t length) {
		return SubstringFun::SubstringGrapheme(result, input, offset, length);
	}
};

template <class OP>
static void SubstringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];
	auto &offset_vector = args.data[1];
	if (args.ColumnCount() == 3) {
		auto &length_vector = args.data[2];

		TernaryExecutor::Execute<string_t, int64_t, int64_t, string_t>(
		    input_vector, offset_vector, length_vector, result, args.size(),
		    [&](string_t input_string, int64_t offset, int64_t length) {
			    return OP::Substring(result, input_string, offset, length);
		    });
	} else {
		BinaryExecutor::Execute<string_t, int64_t, string_t>(
		    input_vector, offset_vector, result, args.size(), [&](string_t input_string, int64_t offset) {
			    return OP::Substring(result, input_string, offset, NumericLimits<uint32_t>::Maximum());
		    });
	}
}

static void SubstringFunctionASCII(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];
	auto &offset_vector = args.data[1];
	if (args.ColumnCount() == 3) {
		auto &length_vector = args.data[2];

		TernaryExecutor::Execute<string_t, int64_t, int64_t, string_t>(
		    input_vector, offset_vector, length_vector, result, args.size(),
		    [&](string_t input_string, int64_t offset, int64_t length) {
			    return SubstringASCII(result, input_string, offset, length);
		    });
	} else {
		BinaryExecutor::Execute<string_t, int64_t, string_t>(
		    input_vector, offset_vector, result, args.size(), [&](string_t input_string, int64_t offset) {
			    return SubstringASCII(result, input_string, offset, NumericLimits<uint32_t>::Maximum());
		    });
	}
}

static unique_ptr<BaseStatistics> SubstringPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	// can only propagate stats if the children have stats
	// we only care about the stats of the first child (i.e. the string)
	if (!StringStats::CanContainUnicode(child_stats[0])) {
		expr.function.function = SubstringFunctionASCII;
	}
	return nullptr;
}

void SubstringFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet substr("substring");
	substr.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                                  LogicalType::VARCHAR, SubstringFunction<SubstringUnicodeOp>, nullptr, nullptr,
	                                  SubstringPropagateStats));
	substr.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                                  SubstringFunction<SubstringUnicodeOp>, nullptr, nullptr,
	                                  SubstringPropagateStats));
	set.AddFunction(substr);
	substr.name = "substr";
	set.AddFunction(substr);

	ScalarFunctionSet substr_grapheme("substring_grapheme");
	substr_grapheme.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                                           LogicalType::VARCHAR, SubstringFunction<SubstringGraphemeOp>, nullptr,
	                                           nullptr, SubstringPropagateStats));
	substr_grapheme.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                                           SubstringFunction<SubstringGraphemeOp>, nullptr, nullptr,
	                                           SubstringPropagateStats));
	set.AddFunction(substr_grapheme);
}

} // namespace duckdb
