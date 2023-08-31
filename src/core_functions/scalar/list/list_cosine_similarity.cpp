#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"

namespace duckdb {

template <class NUMERIC_TYPE>
static void ListCosineSimilarity(DataChunk &args, ExpressionState &, Vector &result) {
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
			    throw InvalidInputException("List lengths must be equal");
		    }

		    NUMERIC_TYPE distance = 0;
		    NUMERIC_TYPE norm_l = 0;
		    NUMERIC_TYPE norm_r = 0;

		    auto l_ptr = left_data + left.offset;
		    auto r_ptr = right_data + right.offset;
		    for (idx_t i = 0; i < left.length; i++) {
			    auto x = *l_ptr++;
			    auto y = *r_ptr++;
			    distance += x * y;
			    norm_l += x * x;
			    norm_r += y * y;
		    }

		    auto similarity = distance / (std::sqrt(norm_l) * std::sqrt(norm_r));

		    // clamp to [-1, 1] to avoid floating point errors
		    if (similarity > 1.0) {
			    similarity = 1.0;
		    } else if (similarity < -1.0) {
			    similarity = -1.0;
		    }
		    return similarity;
	    });

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet ListCosineSimilarityFun::GetFunctions() {
	ScalarFunctionSet set("list_cosine_similarity");
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::FLOAT), LogicalType::LIST(LogicalType::FLOAT)},
	                               LogicalType::FLOAT, ListCosineSimilarity<float>));
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::DOUBLE), LogicalType::LIST(LogicalType::DOUBLE)},
	                               LogicalType::DOUBLE, ListCosineSimilarity<double>));
	return set;
}

} // namespace duckdb
