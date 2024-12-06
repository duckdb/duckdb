#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/core_functions/scalar/string_functions.hpp"
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

unique_ptr<FunctionData> RepeatBindFunction(ClientContext &, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	switch (arguments[0]->return_type.id()) {
	case LogicalTypeId::UNKNOWN:
		throw ParameterNotResolvedException();
	case LogicalTypeId::LIST:
		break;
	default:
		throw NotImplementedException("repeat(list, count) requires a list as parameter");
	}
	bound_function.arguments[0] = arguments[0]->return_type;
	bound_function.return_type = arguments[0]->return_type;
	return nullptr;
}

static void RepeatListFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &list_vector = args.data[0];
	auto &cnt_vector = args.data[1];

	auto &source_child = ListVector::GetEntry(list_vector);
	auto &result_child = ListVector::GetEntry(result);

	idx_t current_size = ListVector::GetListSize(result);
	BinaryExecutor::Execute<list_entry_t, int64_t, list_entry_t>(
	    list_vector, cnt_vector, result, args.size(), [&](list_entry_t list_input, int64_t cnt) {
		    idx_t copy_count = cnt <= 0 || list_input.length == 0 ? 0 : UnsafeNumericCast<idx_t>(cnt);
		    idx_t result_length = list_input.length * copy_count;
		    idx_t new_size = current_size + result_length;
		    ListVector::Reserve(result, new_size);
		    list_entry_t result_list;
		    result_list.offset = current_size;
		    result_list.length = result_length;
		    for (idx_t i = 0; i < copy_count; i++) {
			    // repeat the list contents "cnt" times
			    VectorOperations::Copy(source_child, result_child, list_input.offset + list_input.length,
			                           list_input.offset, current_size);
			    current_size += list_input.length;
		    }
		    return result_list;
	    });
	ListVector::SetListSize(result, current_size);
}

ScalarFunctionSet RepeatFun::GetFunctions() {
	ScalarFunctionSet repeat;
	for (const auto &type : {LogicalType::VARCHAR, LogicalType::BLOB}) {
		repeat.AddFunction(ScalarFunction({type, LogicalType::BIGINT}, type, RepeatFunction));
	}
	repeat.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::BIGINT},
	                                  LogicalType::LIST(LogicalType::ANY), RepeatListFunction, RepeatBindFunction));
	return repeat;
}

} // namespace duckdb
