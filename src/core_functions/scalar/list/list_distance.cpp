#include "duckdb/core_functions/scalar/list_functions.hpp"
#include <cmath>

namespace duckdb {

template <class NUMERIC_TYPE>
static void ListDistance(DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto count = args.size();
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto left_count = ListVector::GetListSize(left);
	auto right_count = ListVector::GetListSize(right);

	UnifiedVectorFormat left_child_format;
	ListVector::GetEntry(left).ToUnifiedFormat(left_count, left_child_format);
	if (!left_child_format.validity.CheckAllValid(left_count)) {
		throw InvalidInputException("list_distance: left argument can not contain NULL values");
	}

	UnifiedVectorFormat right_child_format;
	ListVector::GetEntry(right).ToUnifiedFormat(right_count, right_child_format);
	if (!right_child_format.validity.CheckAllValid(right_count)) {
		throw InvalidInputException("list_distance: right argument can not contain NULL values");
	}

	auto left_data = reinterpret_cast<NUMERIC_TYPE *>(left_child_format.data);
	auto right_data = reinterpret_cast<NUMERIC_TYPE *>(right_child_format.data);

	BinaryExecutor::Execute<list_entry_t, list_entry_t, NUMERIC_TYPE>(
	    left, right, result, count, [&](list_entry_t left, list_entry_t right) {
		    if (left.length != right.length) {
			    throw InvalidInputException(StringUtil::Format(
			        "list_distance: list dimensions must be equal, got left length %d and right length %d", left.length,
			        right.length));
		    }

		    auto dimensions = left.length;

		    NUMERIC_TYPE distance = 0;

		    auto l_ptr = left_data + left.offset;
		    auto r_ptr = right_data + right.offset;

		    for (idx_t i = 0; i < dimensions; i++) {
			    auto x = *l_ptr++;
			    auto y = *r_ptr++;
			    auto diff = x - y;
			    distance += diff * diff;
		    }

		    return std::sqrt(distance);
	    });

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet ListDistanceFun::GetFunctions() {
	ScalarFunctionSet set("list_distance");
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::FLOAT), LogicalType::LIST(LogicalType::FLOAT)},
	                               LogicalType::FLOAT, ListDistance<float>));
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::DOUBLE), LogicalType::LIST(LogicalType::DOUBLE)},
	                               LogicalType::DOUBLE, ListDistance<double>));
	return set;
}

} // namespace duckdb
