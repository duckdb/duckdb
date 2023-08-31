#include "duckdb/core_functions/scalar/list_functions.hpp"
#include <cmath>

namespace duckdb {

template <class NUMERIC_TYPE>
static void ListDistance(DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto count = args.size();
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto &left_child = ListVector::GetEntry(left);
	auto &right_child = ListVector::GetEntry(right);
	auto left_data = FlatVector::GetData<NUMERIC_TYPE>(left_child);
	auto right_data = FlatVector::GetData<NUMERIC_TYPE>(right_child);

	BinaryExecutor::Execute<list_entry_t, list_entry_t, NUMERIC_TYPE>(
	    left, right, result, count, [&](list_entry_t left, list_entry_t right) {
		    if (left.length != right.length) {
			    throw InvalidInputException("List dimensions must be equal");
		    }

		    NUMERIC_TYPE distance = 0;

		    auto l_ptr = left_data + left.offset;
		    auto r_ptr = right_data + right.offset;

		    for (idx_t i = 0; i < left.length; i++) {
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
