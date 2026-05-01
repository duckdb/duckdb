#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "core_functions/scalar/struct_functions.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ClientContext;
struct ExpressionState;

static void StructValuesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	const idx_t count = args.size();

	auto &input_children = StructVector::GetEntries(input);
	auto &result_children = StructVector::GetEntries(result);
	D_ASSERT(result_children.size() == input_children.size());

	// UnnamedStruct vector and Struct vector are actually the same underneath, so we can just reference the children
	if (StructType::IsUnnamed(input.GetType())) {
		result.Reference(input);
		return;
	}

	// We would use result.Reference(input) also for this case,
	// but that function asserts that the logical types are the same
	for (idx_t i = 0; i < input_children.size(); i++) {
		result_children[i].Reference(input_children[i]);
	}

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(input)) {
			ConstantVector::SetNull(result, count_t(count));
		}
	} else {
		// set only the struct buffer's type - do not propagate to children
		// since children reference external vectors (input children) that may have incompatible buffer types
		result.BufferMutable().SetVectorTypeOnly(VectorType::FLAT_VECTOR);

		// Make result validity to mirror input's nulls
		auto validity_entries = input.Validity(count);

		if (validity_entries.CanHaveNull()) {
			auto &validity = FlatVector::ValidityMutable(result);

			for (idx_t i = 0; i < count; i++) {
				if (!validity_entries.IsValid(i)) {
					validity.SetInvalid(i);
				}
			}
		}
	}
}

// Ensure input is a STRUCT, set return type to an unnamed STRUCT with same child types
static unique_ptr<FunctionData> StructValuesBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	const auto arg_type = arguments[0]->GetReturnType();
	if (arg_type == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	// Since the type of the argument we declared of in `GetFunction` doesn't contain the inner STRUCT type,
	// we should take it from the arguments
	bound_function.GetArguments()[0] = arg_type;

	// Build unnamed children list using only types, with empty names
	child_list_t<LogicalType> unnamed_children;
	auto &children = StructType::GetChildTypes(arguments[0]->GetReturnType());
	unnamed_children.reserve(children.size());
	for (auto &child : children) {
		unnamed_children.emplace_back("", child.second);
	}
	bound_function.SetReturnType(LogicalType::STRUCT(unnamed_children));
	return nullptr;
}

ScalarFunction StructValuesFun::GetFunction() {
	ScalarFunction func({LogicalTypeId::STRUCT}, LogicalTypeId::STRUCT, StructValuesFunction, StructValuesBind);
	return func;
}

} // namespace duckdb
