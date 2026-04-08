#include <memory>

#include "duckdb/common/vector/union_vector.hpp"
#include "core_functions/scalar/union_functions.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ClientContext;
struct ExpressionState;

namespace {

unique_ptr<FunctionData> UnionTagBind(ClientContext &context, ScalarFunction &bound_function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw BinderException("Missing required arguments for union_tag function.");
	}

	if (LogicalTypeId::UNKNOWN == arguments[0]->return_type.id()) {
		throw ParameterNotResolvedException();
	}

	if (LogicalTypeId::UNION != arguments[0]->return_type.id()) {
		throw BinderException("First argument to union_tag function must be a union type.");
	}

	if (arguments.size() > 1) {
		throw BinderException("Too many arguments, union_tag takes at most one argument.");
	}

	auto member_count = UnionType::GetMemberCount(arguments[0]->return_type);
	if (member_count == 0) {
		// this should never happen, empty unions are not allowed
		throw InternalException("Can't get tags from an empty union");
	}

	bound_function.arguments[0] = arguments[0]->return_type;

	auto varchar_vector = Vector(LogicalType::VARCHAR, member_count);
	auto result_data = FlatVector::Writer<string_t>(varchar_vector, member_count);
	for (idx_t i = 0; i < member_count; i++) {
		auto str = string_t(UnionType::GetMemberName(arguments[0]->return_type, i));
		result_data[i] = str;
	}
	auto enum_type = LogicalType::ENUM(varchar_vector, member_count);
	bound_function.SetReturnType(enum_type);

	return nullptr;
}

void UnionTagFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::ENUM);
	result.Reinterpret(UnionVector::GetTags(args.data[0]));
}

} // namespace

ScalarFunction UnionTagFun::GetFunction() {
	return ScalarFunction({LogicalTypeId::UNION}, LogicalTypeId::ANY, UnionTagFunction, UnionTagBind, nullptr,
	                      nullptr); // TODO: Statistics?
}

} // namespace duckdb
