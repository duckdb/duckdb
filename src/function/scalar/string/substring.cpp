#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "utf8proc.hpp"

namespace duckdb {

string_t SubstringEmptyString(Vector &result) {
	auto result_string = StringVector::EmptyString(result, 0);
	result_string.Finalize();
	return result_string;
}

string_t SubstringSlice(Vector &result, const char *input_data, int64_t offset, int64_t length) {
	auto result_string = StringVector::EmptyString(result, length);
	auto result_data = result_string.GetDataWriteable();
	memcpy(result_data, input_data + offset, length);
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
		start = MaxValue<int64_t>(0, end + length);
	}
	if (start == end) {
		return false;
	}
	D_ASSERT(start < end);
	return true;
}

string_t SubstringASCII(Vector &result, string_t input, int64_t offset, int64_t length) {
	auto input_data = input.GetDataUnsafe();
	auto input_size = input.GetSize();

	int64_t start, end;
	if (!SubstringStartEnd(input_size, offset, length, start, end)) {
		return SubstringEmptyString(result);
	}
	return SubstringSlice(result, input_data, start, end - start);
}

string_t SubstringFun::SubstringScalarFunction(Vector &result, string_t input, int64_t offset, int64_t length) {
	auto input_data = input.GetDataUnsafe();
	auto input_size = input.GetSize();

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
		if (end_pos == DConstants::INVALID_INDEX) {
			return SubstringEmptyString(result);
		}
	} else {
		start_pos = DConstants::INVALID_INDEX;
		end_pos = input_size;

		// positive offset: scan forwards
		int64_t start, end;

		// we express start and end as unicode codepoints from the front
		if (length < 0) {
			// negative length
			start = MaxValue<int64_t>(0, offset + length - 1);
			end = offset - 1;
		} else {
			// positive length
			start = MaxValue<int64_t>(0, offset - 1);
			end = offset + length - 1;
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
		if (start_pos == DConstants::INVALID_INDEX || end == 0) {
			return SubstringEmptyString(result);
		}
	}
	D_ASSERT(end_pos >= start_pos);
	// after we have found these, we can slice the substring
	return SubstringSlice(result, input_data, start_pos, end_pos - start_pos);
}

static void SubstringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];
	auto &offset_vector = args.data[1];
	if (args.ColumnCount() == 3) {
		auto &length_vector = args.data[2];

		TernaryExecutor::Execute<string_t, int64_t, int64_t, string_t>(
		    input_vector, offset_vector, length_vector, result, args.size(),
		    [&](string_t input_string, int64_t offset, int64_t length) {
			    return SubstringFun::SubstringScalarFunction(result, input_string, offset, length);
		    });
	} else {
		BinaryExecutor::Execute<string_t, int64_t, string_t>(
		    input_vector, offset_vector, result, args.size(), [&](string_t input_string, int64_t offset) {
			    return SubstringFun::SubstringScalarFunction(result, input_string, offset,
			                                                 NumericLimits<int64_t>::Maximum() - offset);
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
			    return SubstringASCII(result, input_string, offset, NumericLimits<int64_t>::Maximum() - offset);
		    });
	}
}

static unique_ptr<BaseStatistics> SubstringPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	// can only propagate stats if the children have stats
	if (!child_stats[0]) {
		return nullptr;
	}
	// we only care about the stats of the first child (i.e. the string)
	auto &sstats = (StringStatistics &)*child_stats[0];
	if (!sstats.has_unicode) {
		expr.function.function = SubstringFunctionASCII;
	}
	return nullptr;
}

void SubstringFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet substr("substring");
	substr.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                                  LogicalType::VARCHAR, SubstringFunction, nullptr, nullptr,
	                                  SubstringPropagateStats));
	substr.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                                  SubstringFunction, nullptr, nullptr, SubstringPropagateStats));
	set.AddFunction(substr);
	substr.name = "substr";
	set.AddFunction(substr);
}

} // namespace duckdb
