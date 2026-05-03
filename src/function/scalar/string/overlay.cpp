#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

namespace {

// Returns the byte offset of the start of the `char_pos`th character (1-based).
// Returns input_size if char_pos is past the end of the string.
// Clamps to 0 if char_pos <= 1.
static idx_t CharPosToByte(const char *data, idx_t input_size, int64_t char_pos) {
	if (char_pos <= 1) {
		return 0;
	}
	int64_t current = 1;
	for (idx_t i = 0; i < input_size; i++) {
		if (IsCharacter(data[i])) {
			if (current == char_pos) {
				return i;
			}
			current++;
		}
	}
	return input_size;
}

static int64_t CharLength(const char *data, idx_t size) {
	int64_t length = 0;
	for (idx_t i = 0; i < size; i++) {
		length += IsCharacter(data[i]);
	}
	return length;
}

static string_t OverlayScalar(Vector &result, string_t input, string_t replacement, int64_t start, int64_t count) {
	auto input_data = input.GetData();
	auto input_size = input.GetSize();
	auto repl_data = replacement.GetData();
	auto repl_size = replacement.GetSize();

	if (count < 0) {
		count = 0;
	}

	idx_t byte_start = CharPosToByte(input_data, input_size, start);
	idx_t byte_end = CharPosToByte(input_data, input_size, start + count);

	idx_t before_size = byte_start;
	idx_t after_size = input_size - byte_end;
	idx_t total_size = before_size + repl_size + after_size;

	auto result_string = StringVector::EmptyString(result, total_size);
	auto result_data = result_string.GetDataWriteable();

	memcpy(result_data, input_data, before_size);
	memcpy(result_data + before_size, repl_data, repl_size);
	memcpy(result_data + before_size + repl_size, input_data + byte_end, after_size);

	result_string.Finalize();
	return result_string;
}

void OverlayFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vec = args.data[0];
	auto &repl_vec = args.data[1];
	auto &start_vec = args.data[2];

	if (args.ColumnCount() == 4) {
		auto &count_vec = args.data[3];
		// Flatten all inputs so we can iterate safely
		UnifiedVectorFormat input_data, repl_data, start_data, count_data;
		input_vec.ToUnifiedFormat(args.size(), input_data);
		repl_vec.ToUnifiedFormat(args.size(), repl_data);
		start_vec.ToUnifiedFormat(args.size(), start_data);
		count_vec.ToUnifiedFormat(args.size(), count_data);

		auto inputs = UnifiedVectorFormat::GetData<string_t>(input_data);
		auto repls = UnifiedVectorFormat::GetData<string_t>(repl_data);
		auto starts = UnifiedVectorFormat::GetData<int64_t>(start_data);
		auto counts = UnifiedVectorFormat::GetData<int64_t>(count_data);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_entries = FlatVector::GetDataMutable<string_t>(result);
		auto &result_validity = FlatVector::ValidityMutable(result);

		for (idx_t i = 0; i < args.size(); i++) {
			auto input_idx = input_data.sel->get_index(i);
			auto repl_idx = repl_data.sel->get_index(i);
			auto start_idx = start_data.sel->get_index(i);
			auto count_idx = count_data.sel->get_index(i);

			if (!input_data.validity.RowIsValid(input_idx) || !repl_data.validity.RowIsValid(repl_idx) ||
			    !start_data.validity.RowIsValid(start_idx) || !count_data.validity.RowIsValid(count_idx)) {
				result_validity.SetInvalid(i);
				continue;
			}
			result_entries[i] =
			    OverlayScalar(result, inputs[input_idx], repls[repl_idx], starts[start_idx], counts[count_idx]);
		}
	} else {
		TernaryExecutor::Execute<string_t, string_t, int64_t, string_t>(
		    input_vec, repl_vec, start_vec, result, args.size(),
		    [&](string_t input, string_t replacement, int64_t start) {
			    int64_t count = CharLength(replacement.GetData(), replacement.GetSize());
			    return OverlayScalar(result, input, replacement, start, count);
		    });
	}
}

} // namespace

ScalarFunctionSet OverlayFun::GetFunctions() {
	ScalarFunctionSet overlay_set("overlay");
	overlay_set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                                       LogicalType::VARCHAR, OverlayFunction));
	overlay_set.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                   LogicalType::VARCHAR, OverlayFunction));
	return overlay_set;
}

} // namespace duckdb
