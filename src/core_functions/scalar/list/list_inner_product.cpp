#include "duckdb/core_functions/scalar/list_functions.hpp"

namespace duckdb {

template <class NUMERIC_TYPE>
static void ListInnerProduct(DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto count = args.size();
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto left_count = ListVector::GetListSize(left);
	auto right_count = ListVector::GetListSize(right);

	auto &left_child = ListVector::GetEntry(left);
	auto &right_child = ListVector::GetEntry(right);

	D_ASSERT(left_child.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(right_child.GetVectorType() == VectorType::FLAT_VECTOR);

	if (!FlatVector::Validity(left_child).CheckAllValid(left_count)) {
		throw InvalidInputException("list_inner_product: left argument can not contain NULL values");
	}

	if (!FlatVector::Validity(right_child).CheckAllValid(right_count)) {
		throw InvalidInputException("list_inner_product: right argument can not contain NULL values");
	}

	auto left_data = FlatVector::GetData<NUMERIC_TYPE>(left_child);
	auto right_data = FlatVector::GetData<NUMERIC_TYPE>(right_child);

	BinaryExecutor::Execute<list_entry_t, list_entry_t, NUMERIC_TYPE>(
	    left, right, result, count, [&](list_entry_t left, list_entry_t right) {
		    if (left.length != right.length) {
			    throw InvalidInputException(StringUtil::Format(
			        "list_inner_product: list dimensions must be equal, got left length %d and right length %d",
			        left.length, right.length));
		    }

		    auto dimensions = left.length;

		    NUMERIC_TYPE distance = 0;

		    auto l_ptr = left_data + left.offset;
		    auto r_ptr = right_data + right.offset;

		    for (idx_t i = 0; i < dimensions; i++) {
			    auto x = *l_ptr++;
			    auto y = *r_ptr++;
			    distance += x * y;
		    }

		    return distance;
	    });

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet ListInnerProductFun::GetFunctions() {
	ScalarFunctionSet set("list_inner_product");
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::FLOAT), LogicalType::LIST(LogicalType::FLOAT)},
	                               LogicalType::FLOAT, ListInnerProduct<float>));
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::DOUBLE), LogicalType::LIST(LogicalType::DOUBLE)},
	                               LogicalType::DOUBLE, ListInnerProduct<double>));
	return set;
}

} // namespace duckdb
