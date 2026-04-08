#include <stdint.h>
#include <memory>

#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ClientContext;
struct ExpressionState;

static void CardinalityFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &map = args.data[0];
	auto entries = map.Values<list_entry_t>(args.size());

	auto result_data = FlatVector::Writer<uint64_t>(result, args.size());
	for (idx_t row = 0; row < args.size(); row++) {
		auto entry = entries[row];
		if (!entry.IsValid()) {
			result_data.SetInvalid(row);
			continue;
		}
		result_data[row] = entries.GetValueUnsafe(row).length;
	}
}

static unique_ptr<FunctionData> CardinalityBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 1) {
		throw BinderException("Cardinality must have exactly one arguments");
	}

	if (arguments[0]->return_type.id() != LogicalTypeId::MAP) {
		throw BinderException("Cardinality can only operate on MAPs");
	}

	bound_function.SetReturnType(LogicalType::UBIGINT);
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

ScalarFunction CardinalityFun::GetFunction() {
	ScalarFunction fun({LogicalType::ANY}, LogicalType::UBIGINT, CardinalityFunction, CardinalityBind);
	fun.varargs = LogicalType::ANY;
	fun.SetNullHandling(FunctionNullHandling::DEFAULT_NULL_HANDLING);
	return fun;
}

} // namespace duckdb
