#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "core_functions/scalar/string_functions.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"

namespace duckdb {

static void RepeatFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &str_vector = args.data[0];
	auto &cnt_vector = args.data[1];

	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    str_vector, cnt_vector, result, args.size(), [&](string_t str, int64_t cnt) {
		    auto input_str = str.GetData();
		    auto size_str = str.GetSize();
		    idx_t copy_count = cnt <= 0 || size_str == 0 ? 0 : UnsafeNumericCast<idx_t>(cnt);

		    idx_t copy_size;
		    if (TryMultiplyOperator::Operation(size_str, copy_count, copy_size)) {
			    auto result_str = StringVector::EmptyString(result, copy_size);
			    auto result_data = result_str.GetDataWriteable();
			    for (idx_t i = 0; i < copy_count; i++) {
				    memcpy(result_data + i * size_str, input_str, size_str);
			    }
			    result_str.Finalize();
			    return result_str;
		    } else {
			    throw OutOfRangeException(
			        "Cannot create a string of size: '%d' * '%d', the maximum supported string size is: '%d'", size_str,
			        copy_count, string_t::MAX_STRING_SIZE);
		    }
	    });
}

static void RepeatListFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &list_vector = args.data[0];
	auto &cnt_vector = args.data[1];
	auto &source_child = ListVector::GetChildMutable(list_vector);
	auto count = args.size();

	auto list_entries = list_vector.Values<list_entry_t>(count);
	auto cnt_entries = cnt_vector.Values<int64_t>(count);

	auto result_writer = FlatVector::Writer<list_entry_t>(result, count);
	for (idx_t i = 0; i < count; i++) {
		auto list_entry = list_entries[i];
		auto cnt_entry = cnt_entries[i];
		if (!list_entry.IsValid() || !cnt_entry.IsValid()) {
			result_writer.WriteNull();
			continue;
		}
		const auto &list_input = list_entry.GetValue();
		const auto cnt = cnt_entry.GetValue();
		const idx_t copy_count = cnt <= 0 || list_input.length == 0 ? 0 : UnsafeNumericCast<idx_t>(cnt);
		idx_t result_length;
		if (!TryMultiplyOperator::Operation(list_input.length, copy_count, result_length)) {
			throw OutOfRangeException("Cannot create a list of size: '%d' * '%d', the result is too large",
			                          list_input.length, copy_count);
		}
		// reserve the worst-case child capacity up front so an absurd target
		// size fails before we enter a 10^N-iteration Append loop
		ListVector::Reserve(result, ListVector::GetListSize(result) + result_length);
		auto list = result_writer.WriteDynamicList();
		for (idx_t j = 0; j < copy_count; j++) {
			list.Append(source_child, *FlatVector::IncrementalSelectionVector(), list_input.offset + list_input.length,
			            list_input.offset, list_input.length);
		}
	}
	result.Verify(count);
}

ScalarFunctionSet RepeatFun::GetFunctions() {
	ScalarFunctionSet repeat;
	for (const auto &type : {LogicalType::VARCHAR, LogicalType::BLOB}) {
		repeat.AddFunction(ScalarFunction({type, LogicalType::BIGINT}, type, RepeatFunction));
	}
	repeat.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::TEMPLATE("T")), LogicalType::BIGINT},
	                                  LogicalType::LIST(LogicalType::TEMPLATE("T")), RepeatListFunction));
	for (auto &func : repeat.functions) {
		func.SetFallible();
	}
	return repeat;
}

} // namespace duckdb
